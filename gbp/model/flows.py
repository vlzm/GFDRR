"""Flow journal: the event schema, builders, log, and its read-models.

Its read-models are the marginal observations. The flow journal is the single
source of truth for what happened in a run. This
module owns one secret -- the shape of a flow event -- on both sides: the
builders that *write* events (:func:`departed_events`, :func:`arrived_events`,
:func:`redirected_events`, :func:`redirect_continuation_events`,
:func:`lost_events`) and the derivations that *read*
the journal back into
marginals (:func:`flows_to_departures` and friends, :func:`observe`). Write and
read live together on purpose: splitting them would leak the column layout
across two modules.

This is the model layer: the shared vocabulary both the loaders (historical
flows) and the simulator (simulated flows) speak. It depends on neither of them,
so all of them can import from it freely.

Inventory and the in-transit working set are *projections* of the journal: the
marginal observations (departures, arrivals, demand, supply, OD matrix) are pure
functions of the journal and the current inventory, so historical and simulated
runs share one definition for each of them.
"""

import dataclasses

import pandas as pd

# ---------------------------------------------------------------------------
# Flow-event schema
# ---------------------------------------------------------------------------
# Two ids place each event inside its trip (Notations.md §0): ``move_id`` is the
# arc index (0..m -- one physical edge; a redirect adds a second arc) and
# ``event_id`` is the event ordinal (0..n). Both are set by the builders at emit
# time, so the live and finalized journals carry them alike. Row uniqueness is
# the pair ``(flow_id, event_id)``.
FLOW_EVENT_COLUMNS = [
    "flow_id",
    "move_id",
    "event_id",
    "period_id",
    "flow_type",
    "event_type",
    "commodity_category",
    "source_id",
    "planned_target_id",
    "realized_target_id",
    "start_period",
    "planned_end_period",
    "realized_end_period",
    "resource_id",
    "quantity",
    "reason",
    "phase_rank",
    "phase_round",
    "step_id",
]

FLOW_EVENT_DTYPES = {
    "flow_id": "string",
    "move_id": "Int64",
    "event_id": "Int64",
    "period_id": "Int64",
    "flow_type": "string",
    "event_type": "string",
    "commodity_category": "string",
    "source_id": "string",
    "planned_target_id": "string",
    "realized_target_id": "string",
    "start_period": "Int64",
    "planned_end_period": "Int64",
    "realized_end_period": "Int64",
    "resource_id": "string",
    "quantity": "Int64",
    "reason": "string",
    "phase_rank": "Int64",
    "phase_round": "Int64",
    "step_id": "Int64",
}

# phase_rank: which inventory phase of a period applied an event's change. It is
# an open-ended integer (not a fixed set): the phases run in rank order, so a
# lower rank is applied first (Notations.md, "step / step_id"). The three ranks
# below are today's user-trip phases; a later phase (such as rebalancing) takes
# 3, 4, ... An event source that runs as an explicit phase (the simulator) stamps
# its phase's rank straight onto the events it emits; a source with no phases
# (the historical loader) stamps it with :func:`phase_rank_by_timing` instead.
DOCK_PREVIOUS_RANK = 0  # dock bikes that left in an earlier period
PERIOD_OWN_RANK = 1  # this period's own departures and stockout losses
DOCK_SAME_RANK = 2  # dock bikes that left and arrive within this same period

# Event types that dock a bike (+1 at ``realized_target_id``). Only a normal
# ``arrived`` lands a bike now -- including the ``arrived`` that ends a redirect's
# second arc, whose ``realized_target_id`` is the station the bike finally
# reached. ``redirected`` is the intermediate *bounce* off a full station and no
# longer docks (see :func:`redirected_events`); ``lost`` docks nowhere and a
# ``departed`` undocks (-1 at ``source_id``), so neither belongs here.
DOCKING_EVENT_TYPES = ["arrived"]


# ---------------------------------------------------------------------------
# Flow-event builders
# ---------------------------------------------------------------------------
def _typed_events(events_df: pd.DataFrame) -> pd.DataFrame:
    """Cast event columns to the canonical dtypes so frames concat cleanly.

    The phase-ordering columns -- ``phase_rank``, ``phase_round`` and
    ``step_id`` -- are not set by the builders, so they are cast only when already
    present (an empty journal carries them; a freshly built event batch does not).
    The emitting phase stamps ``phase_rank`` (and a redirect batch ``phase_round``)
    onto the events after a builder makes them; ``step_id`` is the run-global
    ordinal :func:`finalize_flows` assigns last.
    """
    for col, dtype in FLOW_EVENT_DTYPES.items():
        if col in events_df.columns:
            events_df[col] = events_df[col].astype(dtype)
    return events_df


def departed_events(trips: pd.DataFrame) -> pd.DataFrame:
    """One ``departed`` event row per trip leaving this period (move 0, event 0).

    This is the trip's opening event: a real user departure from a dock. The
    redirect's second-arc departure is built by
    :func:`redirect_continuation_events` instead and carries ``move_id == 1``, so
    readers that count user departures filter on ``move_id == 0``.
    """
    return _typed_events(
        pd.DataFrame(
            {
                "flow_id": trips["flow_id"],
                "move_id": 0,
                "event_id": 0,
                "period_id": trips["start_period"],
                "flow_type": "user_trip",
                "event_type": "departed",
                "commodity_category": trips["commodity_category"],
                "source_id": trips["source_id"],
                "planned_target_id": trips["planned_target_id"],
                "realized_target_id": pd.NA,
                "start_period": trips["start_period"],
                "planned_end_period": trips["planned_end_period"],
                "realized_end_period": pd.NA,
                "resource_id": pd.NA,
                "quantity": 1,
                "reason": pd.NA,
            }
        )
    )


def arrived_events(in_transit_due: pd.DataFrame, period_id: int | pd.Series) -> pd.DataFrame:
    """One ``arrived`` event row per in-transit flow docking at ``period_id`` (move 0, event 1).

    The normal-trip arrival: a bike docked at its planned target. (The arrival
    that ends a redirect's second arc is built by
    :func:`redirect_continuation_events`, with ``move_id == 1``.)

    ``period_id`` is the docking period: a single int when a whole batch docks
    in the same period (the simulator), or a per-row Series of end periods when
    each flow docks at its own time (the historical log).
    """
    return _typed_events(
        pd.DataFrame(
            {
                "flow_id": in_transit_due["flow_id"],
                "move_id": 0,
                "event_id": 1,
                "period_id": period_id,
                "flow_type": "user_trip",
                "event_type": "arrived",
                "commodity_category": in_transit_due["commodity_category"],
                "source_id": in_transit_due["source_id"],
                "planned_target_id": in_transit_due["planned_target_id"],
                "realized_target_id": in_transit_due["planned_target_id"],
                "start_period": in_transit_due["start_period"],
                "planned_end_period": in_transit_due["planned_end_period"],
                "realized_end_period": period_id,
                "resource_id": pd.NA,
                "quantity": 1,
                "reason": pd.NA,
            }
        )
    )


def redirected_events(flows: pd.DataFrame, period_id: int) -> pd.DataFrame:
    """One ``redirected`` *bounce* event per overflow flow (move 0, event 1).

    This closes the trip's first arc: the bike reached its planned target but the
    docks were full, so it bounced and did *not* dock there. It is therefore no
    longer a docking event -- ``realized_target_id`` is NA and the bike's real
    docking is the ``arrived`` that ends its second arc (see
    :func:`redirect_continuation_events`). ``reason`` records why it bounced.

    Parameters
    ----------
    flows : pandas.DataFrame
        The overflow flows that were redirected, carrying ``source_id``,
        ``planned_target_id``, ``commodity_category``, ``start_period`` and
        ``planned_end_period``.
    period_id : int
        The period the bounce happened in (the first arc ends here).

    Returns
    -------
    pandas.DataFrame
        One ``redirected`` flow-event row per flow.
    """
    return _typed_events(
        pd.DataFrame(
            {
                "flow_id": flows["flow_id"],
                "move_id": 0,
                "event_id": 1,
                "period_id": period_id,
                "flow_type": "user_trip",
                "event_type": "redirected",
                "commodity_category": flows["commodity_category"],
                "source_id": flows["source_id"],
                "planned_target_id": flows["planned_target_id"],
                "realized_target_id": pd.NA,
                "start_period": flows["start_period"],
                "planned_end_period": flows["planned_end_period"],
                "realized_end_period": period_id,
                "resource_id": pd.NA,
                "quantity": 1,
                "reason": "dock_full",
            }
        )
    )


def redirect_continuation_events(redirected: pd.DataFrame, period_id: int) -> pd.DataFrame:
    """Two move-1 rows per redirected flow: the redirect's second arc (``departed`` + ``arrived``).

    After a bike bounces off its full planned target B (the ``redirected`` event,
    move 0), it travels on to the free station C found for it. That second leg is
    a real arc, so it gets its own ``departed`` (move 1, event 2) and ``arrived``
    (move 1, event 3), both in this same period. The leg is pure transport: the
    bike never occupied a dock at B (B was full), so its move-1 ``departed`` is
    *not* a user departure and must be ignored by every reader that counts one
    (they filter ``move_id == 0``). The move-1 ``arrived`` docks the bike at C,
    the single ``+1`` of the whole redirect.

    Parameters
    ----------
    redirected : pandas.DataFrame
        The overflow flows that were redirected, carrying ``flow_id``,
        ``commodity_category``, ``planned_target_id`` (B, the full station),
        ``realized_target_id`` (C, the station found for them) and ``start_period``
        (the flow's opening period, copied onto the continuation rows).
    period_id : int
        The period the redirect happens in (departure from B and docking at C
        both fall here).

    Returns
    -------
    pandas.DataFrame
        Two rows per redirected flow: a move-1 ``departed`` then a move-1
        ``arrived``, ready to append to the journal.
    """
    full_station = redirected["planned_target_id"]  # B: source of the second leg
    docked_at = redirected["realized_target_id"]  # C: where the bike docks
    # ``start_period`` is the flow's opening period on every row of the flow, the
    # move-1 continuation included -- it is when the *flow* departed, not when the
    # second arc starts. The second arc's own timing (its bounce and docking) is
    # ``period_id`` here, both this period. (If a second arc ever needs its own
    # start, add an ``arc_start_period`` column rather than overloading this one.)
    common = {
        "flow_id": redirected["flow_id"],
        "period_id": period_id,
        "flow_type": "user_trip",
        "commodity_category": redirected["commodity_category"],
        "source_id": full_station,
        "planned_target_id": docked_at,
        "start_period": redirected["start_period"],
        "planned_end_period": period_id,
        "realized_end_period": period_id,
        "resource_id": pd.NA,
        "quantity": 1,
        "reason": pd.NA,
    }
    departed = _typed_events(
        pd.DataFrame(
            {
                **common,
                "move_id": 1,
                "event_id": 2,
                "event_type": "departed",
                "realized_target_id": pd.NA,
            }
        )
    )
    arrived = _typed_events(
        pd.DataFrame(
            {
                **common,
                "move_id": 1,
                "event_id": 3,
                "event_type": "arrived",
                "realized_target_id": docked_at,
            }
        )
    )
    return pd.concat([departed, arrived], ignore_index=True)


def lost_events(losses: pd.DataFrame, period_id: int, reason: str) -> pd.DataFrame:
    """One ``lost`` event per trip that did not happen, tagged with ``reason``.

    Serves both loss sites with a single shape: the caller passes whatever columns
    it has and the rest default to NA. The two losses are deliberately different
    shapes because they are different things:

    - a **stockout** loss (source side) is demand that never became a flow,
      aggregated per ``(source_id, commodity_category)`` with ``quantity`` = the
      lost demand and no ``flow_id`` or target;
    - a **dock-full** loss (target side) is a flow that departed but never
      docked: one row per in-transit ``flow_id`` with ``quantity`` = 1, carrying
      its ``source_id`` and ``planned_target_id``.

    ``lost`` events never touch inventory (a stockout bike never left; a dock-full
    bike already left at ``departed`` and docks nowhere). They are pure
    accounting: they make a loss visible in the journal and close a dock-full
    flow's lifecycle. ``realized_target_id`` and ``realized_end_period`` are always NA
    -- a lost trip reaches no target.

    Parameters
    ----------
    losses : pandas.DataFrame
        The lost trips. Always carries ``source_id``, ``commodity_category`` and
        ``quantity``; a dock-full batch also carries ``flow_id``,
        ``planned_target_id``, ``start_period`` and ``planned_end_period``.
    period_id : int
        The period the loss is recorded in.
    reason : str
        Why the trip was lost: ``"stockout"`` or ``"dock_full"``.

    Returns
    -------
    pandas.DataFrame
        One ``lost`` flow-event row per input row.
    """
    # The reason fixes the event's place in its trip (Notations.md §0): a stockout
    # is the trip's first and only event (move 0, event 0 -- no departure preceded
    # it), a dock-full loss closes a flow that already departed (move 0, event 1).
    # ``move_id`` is 0 either way: a stockout has no arc and a dock-full loss has
    # the single arc of an ordinary trip.
    event_id = 0 if reason == "stockout" else 1
    na = pd.Series([pd.NA] * len(losses), index=losses.index)
    return _typed_events(
        pd.DataFrame(
            {
                "flow_id": losses.get("flow_id", na),
                "move_id": 0,
                "event_id": event_id,
                "period_id": period_id,
                "flow_type": "user_trip",
                "event_type": "lost",
                "commodity_category": losses["commodity_category"],
                "source_id": losses["source_id"],
                "planned_target_id": losses.get("planned_target_id", na),
                "realized_target_id": pd.NA,
                "start_period": losses.get("start_period", na),
                "planned_end_period": losses.get("planned_end_period", na),
                "realized_end_period": pd.NA,
                "resource_id": pd.NA,
                "quantity": losses["quantity"],
                "reason": reason,
            }
        )
    )


def empty_in_transit() -> pd.DataFrame:
    """Empty in-transit table (a ``departed``-event frame with no rows)."""
    return departed_events(
        pd.DataFrame(
            {
                "flow_id": pd.Series(dtype="string"),
                "source_id": pd.Series(dtype="string"),
                "planned_target_id": pd.Series(dtype="string"),
                "commodity_category": pd.Series(dtype="string"),
                "start_period": pd.Series(dtype="Int64"),
                "planned_end_period": pd.Series(dtype="Int64"),
            }
        )
    )


# ---------------------------------------------------------------------------
# Flow journal (the run's single source of truth)
# ---------------------------------------------------------------------------
def empty_flows_journal() -> pd.DataFrame:
    """Empty append-only flow journal.

    Holds the canonical flow-event columns (:data:`FLOW_EVENT_COLUMNS`). Every
    event carries its ``move_id`` and ``event_id`` from the moment a builder
    creates it, so the growing journal already has them and
    :func:`finalize_flows` only sorts and projects -- it assigns no id.
    """
    return pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in FLOW_EVENT_DTYPES.items()})


def phase_rank_by_timing(flows: pd.DataFrame) -> pd.Series:
    """Derive each event's ``phase_rank`` from its own columns (the write-time rule).

    This is for an event source that has **no** explicit phases: the historical
    loader turns raw trips into a journal in one pass, so it stamps ``phase_rank``
    with this rule rather than knowing it from a phase. The simulator, which runs
    real phases, stamps its phase's rank directly and never calls this.

    The rule matches the simulator's phase order (dock-previous -> form departures
    -> dock-same). ``t`` is the flow's opening period, which is ``start_period`` on
    every row of a flow (the move-1 continuation included):

    - :data:`DOCK_PREVIOUS_RANK` (0) -- a docking-phase event for a flow that
      opened in an **earlier** period (``period_id > t``).
    - :data:`PERIOD_OWN_RANK` (1) -- the period's own activity: a real user
      ``departed`` (``move_id == 0``, the ``-1``) and a stockout ``lost`` (touches
      no inventory).
    - :data:`DOCK_SAME_RANK` (2) -- a docking-phase event for a flow that opened in
      **this** period (``period_id == t``).

    A docking-phase event is anything emitted while docking arrivals: an
    ``arrived`` (either arc), a ``redirected`` bounce, a redirect's move-1
    ``departed``, or a dock-full ``lost``. A docking-phase event can never precede
    its own flow's departure, so ``period_id >= t`` always and the two docking
    cases above are exhaustive.

    The rule and the simulator's stamped constants must agree; the scenario test
    ``test_step_id_is_a_pure_function_of_the_journal`` uses this function as the
    independent oracle that locks that agreement.
    """
    is_departure = (flows["event_type"] == "departed") & (flows["move_id"] == 0)
    is_stockout = (flows["event_type"] == "lost") & (flows["reason"] == "stockout")
    is_period_own = is_departure | is_stockout
    rank = pd.Series(DOCK_PREVIOUS_RANK, index=flows.index, dtype="int64")
    rank = rank.mask(is_period_own, PERIOD_OWN_RANK)
    rank = rank.mask(~is_period_own & (flows["period_id"] == flows["start_period"]), DOCK_SAME_RANK)
    return rank


def _assign_step_id(flows: pd.DataFrame) -> pd.DataFrame:
    """Set ``step_id`` and ``phase_round`` labels and return ``flows`` in step order.

    ``step_id`` is the run-global ordinal of an inventory step -- one batch of
    ``+1`` / ``-1`` applied together (Notations.md §0.1). It is filled one of two
    ways, depending on whether the producer already stamped it:

    - **Stamped (the simulator).** Each phase opens a step at apply time
      (:meth:`SimulationState.open_step`) and writes its number onto the events of
      that step, so every event arrives with a ``step_id``. Here we trust it and
      only sort by it. The number was opened from a run-global counter, never
      derived from the columns, so two ordered batches can never share it.
    - **Derived (the historical loader).** The loader has no phases and stamps no
      ``step_id``, so it arrives absent (or all-NA). We then number the distinct
      ``(period_id, phase_rank, phase_round)`` tuples 0, 1, 2, ... in sorted order.
      This is safe because history is pure user trips -- no redirects, no
      rebalancing -- so one tuple is always exactly one batch.

    Either way ``phase_rank`` and ``phase_round`` stay on every row as labels (when
    / which phase / which round):

    - ``phase_rank`` orders the phases inside a period (an open-ended integer:
      today 0/1/2 for user trips, later phases take 3, 4, ...). The emitting phase
      stamps it (the historical loader uses :func:`phase_rank_by_timing`), so this
      reads the stored column; any row that arrives without one is filled by the
      rule as a safety net.
    - ``phase_round`` orders the rounds inside one phase that applies several
      ordered batches: 0 for a single-batch phase, 1.. for each later round (a
      redirect's rounds today). It is the one piece of order not recoverable from
      the other columns (the rounds are the mechanics' internal iteration), so the
      phase stores it; rows without it carry 0.

    Events that share a step share a ``step_id`` either way.
    """
    if "phase_rank" in flows.columns:
        phase_rank = flows["phase_rank"]
        missing = phase_rank.isna()
        if missing.any():
            phase_rank = phase_rank.where(~missing, phase_rank_by_timing(flows))
        phase_rank = phase_rank.astype("int64")
    else:
        phase_rank = phase_rank_by_timing(flows)
    if "phase_round" in flows.columns:
        phase_round = flows["phase_round"].fillna(0).astype("int64")
    else:
        phase_round = pd.Series(0, index=flows.index, dtype="int64")

    stamped = "step_id" in flows.columns and not flows["step_id"].isna().all()
    if stamped:
        # Trust the stamped number; order by it, then by the trip ids within a step.
        order = pd.DataFrame(
            {
                "step_id": flows["step_id"].astype("int64"),
                "phase_rank": phase_rank,
                "phase_round": phase_round,
                "flow_id": flows["flow_id"],
                "event_id": flows["event_id"],
            }
        )
        order = order.sort_values(["step_id", "flow_id", "event_id"], kind="stable")
        flows = flows.loc[order.index].copy()
        flows["phase_rank"] = order["phase_rank"].to_numpy()
        flows["phase_round"] = order["phase_round"].to_numpy()
        flows["step_id"] = order["step_id"].to_numpy()
        return flows.reset_index(drop=True)

    # Derive: number the distinct (period_id, phase_rank, phase_round) tuples.
    keys = ["period_id", "phase_rank", "phase_round"]
    order = pd.DataFrame(
        {
            "period_id": flows["period_id"],
            "phase_rank": phase_rank,
            "phase_round": phase_round,
            "flow_id": flows["flow_id"],
            "event_id": flows["event_id"],
        }
    )
    order = order.sort_values([*keys, "flow_id", "event_id"], kind="stable")
    flows = flows.loc[order.index].copy()
    flows["phase_rank"] = order["phase_rank"].to_numpy()
    flows["phase_round"] = order["phase_round"].to_numpy()
    # ngroup over the sorted frame numbers the distinct (period_id, phase_rank,
    # phase_round) batches 0, 1, 2, ... in order of appearance -- step order.
    flows["step_id"] = order.groupby(keys, sort=False).ngroup().to_numpy()
    return flows.reset_index(drop=True)


def finalize_flows(journal: pd.DataFrame) -> pd.DataFrame:
    """Order the accumulated journal, assign ``step_id``, and project the columns.

    Both the historical log (:func:`dataloader_graph.get_historical_flows_df`)
    and a replay run's journal are finalized through this one function, so they
    share their order and their column projection by construction rather than by
    two definitions kept in sync by hand. The trip ids (``move_id``, ``event_id``)
    and ``phase_rank`` are already set at emit time (the emitting phase stamps
    ``phase_rank``; the historical loader uses :func:`phase_rank_by_timing`).
    :func:`_assign_step_id` then settles ``step_id`` -- the inventory-step ordinal
    (Notations.md §0.1): it trusts the number the simulator stamped at apply time
    and only derives it from the ``(period_id, phase_rank, phase_round)`` tuple
    when none was stamped (the historical loader). It also fills ``phase_round``
    with 0 wherever a builder did not set it.

    Parameters
    ----------
    journal : pandas.DataFrame
        The append-only journal accumulated during a run.

    Returns
    -------
    pandas.DataFrame
        Event log with columns :data:`FLOW_EVENT_COLUMNS`, ordered by ``step_id``
        then ``flow_id`` then ``event_id``. Within a flow the ``event_id`` order
        matches the events' time order, so each trip's events stay in sequence.
    """
    flows = journal.copy()
    for col, dtype in FLOW_EVENT_DTYPES.items():
        if col in flows.columns:
            flows[col] = flows[col].astype(dtype)
    flows = _assign_step_id(flows)
    flows["phase_rank"] = flows["phase_rank"].astype("Int64")
    flows["phase_round"] = flows["phase_round"].astype("Int64")
    flows["step_id"] = flows["step_id"].astype("Int64")
    return flows[FLOW_EVENT_COLUMNS]


# ---------------------------------------------------------------------------
# Observation derivations (pure functions of the flow journal)
# ---------------------------------------------------------------------------
def flows_to_departures(flows: pd.DataFrame) -> pd.DataFrame:
    """Outflow per period and source: count of move-0 ``departed`` events.

    Grouped by ``(period_id, facility_id, commodity_category)`` where
    ``facility_id`` is the trip's ``source_id``. Only ``move_id == 0`` departures
    count -- those are real user departures from a dock. A redirect's move-1
    ``departed`` is a pure transport leg (the bike never occupied a dock at the
    full station it left), so it is not outflow and is filtered out.
    """
    departed = flows[(flows["event_type"] == "departed") & (flows["move_id"] == 0)]
    return (
        departed.groupby(["period_id", "source_id", "commodity_category"], as_index=False)[
            "quantity"
        ]
        .sum()
        .rename(columns={"source_id": "facility_id"})
    )


def flows_to_arrivals(flows: pd.DataFrame) -> pd.DataFrame:
    """Inflow per period and target: count of docking events.

    A docking is an ``arrived`` event (see :data:`DOCKING_EVENT_TYPES`); it lands
    a bike at its ``realized_target_id``. A redirected bike's inflow is the
    ``arrived`` that ends its second arc (its ``realized_target_id`` is the
    station it finally reached), so redirect inflows still land at the right
    facility. Grouped by ``(period_id, facility_id, commodity_category)`` where
    ``facility_id`` is that ``realized_target_id``.
    """
    docked = flows[flows["event_type"].isin(DOCKING_EVENT_TYPES)]
    keys = ["period_id", "realized_target_id", "commodity_category"]
    return (
        docked.groupby(keys, as_index=False)["quantity"]
        .sum()
        .rename(columns={"realized_target_id": "facility_id"})
    )


def flows_to_od_matrix(flows: pd.DataFrame) -> pd.DataFrame:
    # Only move-0 departures are intended trips. A redirect's move-1 departure is
    # a forced transport leg (duration 0) and would pollute the demand model.
    dep = flows[(flows["event_type"] == "departed") & (flows["move_id"] == 0)].copy()
    dep["duration"] = dep["planned_end_period"] - dep["start_period"]
    od = dep.groupby(
        ["source_id", "planned_target_id", "period_id", "commodity_category"], as_index=False
    ).agg(count=("quantity", "sum"), duration=("duration", "mean"))
    totals = od.groupby(["source_id", "period_id", "commodity_category"])["count"].transform("sum")
    od["probability"] = od["count"] / totals
    od["duration"] = od["duration"].round().astype("Int64")
    return od


def get_inventory_df(flows: pd.DataFrame, initial_inventory: pd.DataFrame) -> pd.DataFrame:
    """Per-period inventory as a pure function of the journal and initial inventory.

    Inventory at the end of period ``t`` equals the initial inventory plus the
    cumulative net flow up to and including ``t``, per
    ``(facility_id, commodity_category)``: a docking ``arrived`` is ``+1`` at its
    ``realized_target_id`` (see :data:`DOCKING_EVENT_TYPES`) and a real user
    ``departed`` (``move_id == 0``) is ``-1`` at its ``source_id``. A redirect's
    move-1 ``departed`` undocks nothing (the bike never sat in a dock at the full
    station) and the ``redirected`` bounce docks nothing, so both are skipped; the
    redirect's net effect is the move-0 ``departed`` (``-1``) and its move-1
    ``arrived`` (``+1`` at the station it reached). ``lost`` events touch no
    facility. Because both historical and
    simulated inventory are defined this way, they need no per-period snapshot
    table — the journal is enough.

    Parameters
    ----------
    flows : pandas.DataFrame
        A flow-event log (historical or finalized simulated).
    initial_inventory : pandas.DataFrame
        Starting inventory with ``facility_id``, ``commodity_category``, ``quantity``.

    Returns
    -------
    pandas.DataFrame
        Columns ``period_id``, ``facility_id``, ``commodity_category``,
        ``quantity_sop`` (start-of-period stock: what is on hand before the
        period's own flows, equal to the previous period's end value, and the
        initial inventory for period 0) and ``quantity_eop`` (end-of-period
        stock: after the period's own flows). One row per
        ``(period, facility, commodity)`` for every period in
        ``[0, max(period_id)]``.
    """
    if flows.empty:
        return pd.DataFrame(
            {
                "period_id": pd.Series(dtype="int64"),
                "facility_id": pd.Series(dtype="string"),
                "commodity_category": pd.Series(dtype="string"),
                "quantity_sop": pd.Series(dtype="int64"),
                "quantity_eop": pd.Series(dtype="int64"),
            }
        )

    dep = (
        flows[(flows["event_type"] == "departed") & (flows["move_id"] == 0)]
        .groupby(["period_id", "source_id", "commodity_category"], as_index=False)["quantity"]
        .sum()
        .rename(columns={"source_id": "facility_id", "quantity": "delta"})
    )
    dep["delta"] = -dep["delta"]
    dock_keys = ["period_id", "realized_target_id", "commodity_category"]
    arr = (
        flows[flows["event_type"].isin(DOCKING_EVENT_TYPES)]
        .groupby(dock_keys, as_index=False)["quantity"]
        .sum()
        .rename(columns={"realized_target_id": "facility_id", "quantity": "delta"})
    )
    deltas = pd.concat([dep, arr], ignore_index=True)
    deltas = deltas.groupby(["period_id", "facility_id", "commodity_category"], as_index=False)[
        "delta"
    ].sum()

    n_periods = int(flows["period_id"].max()) + 1
    net = deltas.pivot_table(
        index=["facility_id", "commodity_category"],
        columns="period_id",
        values="delta",
        fill_value=0,
        aggfunc="sum",
    ).reindex(columns=range(n_periods), fill_value=0)
    net.columns.name = "period_id"

    initial = initial_inventory.set_index(["facility_id", "commodity_category"])["quantity"]
    full_index = net.index.union(initial.index)
    net = net.reindex(full_index, fill_value=0)
    # End-of-period stock: initial plus the running total of net flow up to and
    # including each period. Start-of-period stock is that minus the period's own
    # net flow, so it equals the previous period's end value (and the initial
    # inventory for period 0).
    eop = net.cumsum(axis=1).add(initial.reindex(full_index).fillna(0), axis=0)
    sop = eop - net

    inventory = pd.concat(
        [sop.stack().rename("quantity_sop"), eop.stack().rename("quantity_eop")], axis=1
    ).reset_index()
    inventory["quantity_sop"] = inventory["quantity_sop"].astype("int64")
    inventory["quantity_eop"] = inventory["quantity_eop"].astype("int64")
    return inventory[
        ["period_id", "facility_id", "commodity_category", "quantity_sop", "quantity_eop"]
    ]


def _inventory_deltas(flows: pd.DataFrame) -> pd.DataFrame:
    """One signed ``delta`` per inventory-moving event, at the facility it touches.

    The same rule the inventory derivations use: a docking ``arrived`` is ``+1``
    at its ``realized_target_id`` and a real user ``departed`` (``move_id == 0``)
    is ``-1`` at its ``source_id``; every other event moves no inventory and is
    dropped. Each row keeps its ``step_id`` so the deltas can be cumulated in
    inventory-step order (Notations.md §0.1).
    """
    cols = ["step_id", "period_id", "facility_id", "commodity_category", "delta"]
    dock = flows[flows["event_type"].isin(DOCKING_EVENT_TYPES)].copy()
    dock["facility_id"] = dock["realized_target_id"]
    dock["delta"] = 1
    dep = flows[(flows["event_type"] == "departed") & (flows["move_id"] == 0)].copy()
    dep["facility_id"] = dep["source_id"]
    dep["delta"] = -1
    return pd.concat([dock[cols], dep[cols]], ignore_index=True)


def inventory_at_moments(flows: pd.DataFrame, initial_inventory: pd.DataFrame) -> pd.DataFrame:
    """Inventory just before and just after every inventory step, for all facilities.

    The fine-grained companion of :func:`get_inventory_df`: where that gives one
    value per period, this gives one value per *step* (Notations.md §0.1) -- the
    full cross-section of every ``(facility, commodity)`` at each ``step_id``,
    with its ``inventory_before`` and ``inventory_after``. A facility with no event
    in a step keeps its value (forward-filled by the cumulative sum), so any
    neighbour's inventory at the moment of a redirect is read straight off this
    table. Per-period inventory is the value at each period's last step.

    Like :func:`get_inventory_df` it is a pure function of the journal and the
    initial inventory: ``inventory_after`` at a step is the initial inventory plus
    the cumulative ``+1`` / ``-1`` up to and including that step, and
    ``inventory_before`` is ``inventory_after`` minus the step's own delta.

    Parameters
    ----------
    flows : pandas.DataFrame
        A finalized flow-event log (it must carry ``step_id``).
    initial_inventory : pandas.DataFrame
        Starting inventory with ``facility_id``, ``commodity_category``, ``quantity``.

    Returns
    -------
    pandas.DataFrame
        Columns ``step_id``, ``period_id``, ``facility_id``, ``commodity_category``,
        ``inventory_before``, ``inventory_after`` -- one row per (step, facility,
        commodity), sorted by ``step_id``.
    """
    empty = pd.DataFrame(
        {
            "step_id": pd.Series(dtype="int64"),
            "period_id": pd.Series(dtype="int64"),
            "facility_id": pd.Series(dtype="string"),
            "commodity_category": pd.Series(dtype="string"),
            "inventory_before": pd.Series(dtype="int64"),
            "inventory_after": pd.Series(dtype="int64"),
        }
    )
    if flows.empty:
        return empty

    deltas = _inventory_deltas(flows)
    steps = sorted(int(s) for s in flows["step_id"].dropna().unique())
    # Each step belongs to exactly one period; keep the map for the output.
    step_period = (
        flows.dropna(subset=["step_id"])
        .drop_duplicates("step_id")
        .set_index("step_id")["period_id"]
    )

    # Per-step delta per facility, then the running total across steps gives the
    # "after" value; the matrix is reindexed over every step so a facility that
    # does not move in a step still carries its value forward.
    step_delta = deltas.pivot_table(
        index=["facility_id", "commodity_category"],
        columns="step_id",
        values="delta",
        fill_value=0,
        aggfunc="sum",
    ).reindex(columns=steps, fill_value=0)

    initial = initial_inventory.set_index(["facility_id", "commodity_category"])["quantity"]
    full_index = step_delta.index.union(initial.index)
    step_delta = step_delta.reindex(full_index, fill_value=0)
    after = step_delta.cumsum(axis=1).add(initial.reindex(full_index).fillna(0), axis=0)
    before = after - step_delta

    long_after = after.stack().rename("inventory_after")
    long_before = before.stack().rename("inventory_before")
    out = pd.concat([long_before, long_after], axis=1).reset_index()
    out["period_id"] = out["step_id"].map(step_period).astype("int64")
    out["inventory_before"] = out["inventory_before"].astype("int64")
    out["inventory_after"] = out["inventory_after"].astype("int64")
    return out.sort_values(["step_id", "facility_id", "commodity_category"])[
        [
            "step_id",
            "period_id",
            "facility_id",
            "commodity_category",
            "inventory_before",
            "inventory_after",
        ]
    ].reset_index(drop=True)


def flows_with_inventory(flows: pd.DataFrame, initial_inventory: pd.DataFrame) -> pd.DataFrame:
    """Widen the journal with the inventory of each event's own facility.

    Every event row gains ``inventory_before`` and ``inventory_after`` for the
    ``(facility, commodity)`` it changes -- ``source_id`` for a move-0 ``departed``
    (the ``-1``), ``realized_target_id`` for a docking ``arrived`` (the ``+1``).
    Events that move no inventory (a redirect bounce, a move-1 ``departed``, a
    ``lost``) touch no facility, so their two inventory columns are NA. This is the
    "did this event change inventory correctly?" view; for a neighbour's inventory
    at the same moment, slice :func:`inventory_at_moments` at the event's ``step_id``.

    Parameters
    ----------
    flows : pandas.DataFrame
        A finalized flow-event log (it must carry ``step_id``).
    initial_inventory : pandas.DataFrame
        Starting inventory with ``facility_id``, ``commodity_category``, ``quantity``.

    Returns
    -------
    pandas.DataFrame
        ``flows`` plus a ``facility_id`` (the event's own facility, NA if none),
        ``inventory_before`` and ``inventory_after``.
    """
    moments = inventory_at_moments(flows, initial_inventory)
    out = flows.copy()
    is_dock = out["event_type"].isin(DOCKING_EVENT_TYPES)
    is_dep = (out["event_type"] == "departed") & (out["move_id"] == 0)
    out["facility_id"] = pd.Series(pd.NA, index=out.index, dtype="string")
    out.loc[is_dock, "facility_id"] = out.loc[is_dock, "realized_target_id"]
    out.loc[is_dep, "facility_id"] = out.loc[is_dep, "source_id"]
    out = out.merge(
        moments[
            ["step_id", "facility_id", "commodity_category", "inventory_before", "inventory_after"]
        ],
        on=["step_id", "facility_id", "commodity_category"],
        how="left",
    )
    # Nullable Int64 so the no-facility rows stay <NA>, not a float NaN.
    out["inventory_before"] = out["inventory_before"].astype("Int64")
    out["inventory_after"] = out["inventory_after"].astype("Int64")
    return out


def _squared_distances(geo: pd.DataFrame, origin_id: str) -> pd.Series:
    """Squared Euclidean distance on (lat, lng) from ``origin_id`` to every other facility.

    The same metric the redirect mechanics ranks neighbours by
    (``_nearest_free_station``), so the order here matches the order a redirect
    actually walks. The origin itself is dropped. Squared distance keeps the
    ranking exact without a square root (the order is identical).
    """
    coords = geo.set_index("facility_id")[["lat", "lng"]]
    o = coords.loc[origin_id]
    d2 = (coords["lat"] - o["lat"]) ** 2 + (coords["lng"] - o["lng"]) ** 2
    return d2.drop(index=origin_id).sort_values()


def redirect_neighbor_table(
    flows: pd.DataFrame,
    initial_inventory: pd.DataFrame,
    geo: pd.DataFrame,
    flow_id: str,
    *,
    capacities: pd.DataFrame | None = None,
    n_neighbors: int | None = None,
) -> pd.DataFrame:
    """Explain one redirect: its full station's neighbours, in distance order, at the moment.

    A redirected bike bounced off its full planned station ``B`` and docked at a
    farther station ``C``. To check that landing was right, you want to see, *at
    the redirect's moment*, every station between ``B`` and ``C`` in distance
    order: the nearer ones should have been full (no free dock), and ``C`` the
    first with room. This returns exactly that table -- ``B``'s neighbours ranked
    by distance out to ``C`` (inclusive), each with its dock occupancy just before
    and just after the redirect step (Notations.md §0.1).

    Occupancy is the total bikes docked across commodities, because the docks are
    shared (the same total the redirect's ``free_docks`` uses); pass ``capacities``
    to also get ``capacity`` and the free docks before/after, which is what makes
    "this neighbour was full" visible at a glance.

    Parameters
    ----------
    flows : pandas.DataFrame
        A finalized flow-event log (it must carry ``step_id``).
    initial_inventory : pandas.DataFrame
        Starting inventory with ``facility_id``, ``commodity_category``, ``quantity``.
    geo : pandas.DataFrame
        Facility geography: ``facility_id``, ``lat``, ``lng``.
    flow_id : str
        The redirected flow to explain. It must have a ``redirected`` event.
    capacities : pandas.DataFrame, optional
        Dock capacities (``facility_id``, ``capacity``). When given, the output
        adds ``capacity``, ``free_before`` and ``free_after``.
    n_neighbors : int, optional
        Keep only the ``n_neighbors`` nearest. Default: keep every neighbour out
        to ``C`` (the station the bike actually reached), inclusive.

    Returns
    -------
    pandas.DataFrame
        One row per neighbour, nearest first: ``flow_id``, ``step_id``,
        ``period_id``, ``planned_target_id`` (B), ``realized_target_id`` (C),
        ``commodity_category``, ``neighbor_rank``, ``facility_id``, ``distance_sq``,
        ``inventory_before``, ``inventory_after`` (and the capacity columns above
        when ``capacities`` is given).
    """
    one = flows[flows["flow_id"] == flow_id]
    bounce = one[one["event_type"] == "redirected"]
    if bounce.empty:
        raise ValueError(f"flow {flow_id!r} has no redirect (no 'redirected' event)")
    bounce = bounce.iloc[0]
    full_station = bounce["planned_target_id"]  # B: the full station it bounced off
    step_id = int(bounce["step_id"])
    period_id = int(bounce["period_id"])
    commodity = bounce["commodity_category"]
    docked = one[(one["event_type"] == "arrived") & (one["move_id"] == 1)]
    realized = docked["realized_target_id"].iloc[0] if not docked.empty else pd.NA  # C

    # Neighbours of B by distance, cut at C (inclusive) or the first n_neighbors.
    distances = _squared_distances(geo, full_station)
    order = list(distances.index)
    if n_neighbors is not None:
        order = order[:n_neighbors]
    elif pd.notna(realized) and realized in order:
        order = order[: order.index(realized) + 1]

    # Dock occupancy at the redirect step, summed across commodities (shared docks).
    # inventory_at_moments lists every facility at every step, so a neighbour with
    # no bikes still appears; reindex fills any that never held one with 0.
    moments = inventory_at_moments(flows, initial_inventory)
    at_step = moments[moments["step_id"] == step_id]
    occ = (
        at_step.groupby("facility_id")[["inventory_before", "inventory_after"]]
        .sum()
        .reindex(order, fill_value=0)
    )

    out = pd.DataFrame(
        {
            "flow_id": flow_id,
            "step_id": step_id,
            "period_id": period_id,
            "planned_target_id": full_station,
            "realized_target_id": realized,
            "commodity_category": commodity,
            "neighbor_rank": range(len(order)),
            "facility_id": pd.array(order, dtype="string"),
            "distance_sq": distances.reindex(order).to_numpy(),
            "inventory_before": occ["inventory_before"].to_numpy(),
            "inventory_after": occ["inventory_after"].to_numpy(),
        }
    )
    if capacities is not None:
        cap = capacities.set_index("facility_id")["capacity"]
        out["capacity"] = out["facility_id"].map(cap).astype("Int64")
        out["free_before"] = (out["capacity"] - out["inventory_before"]).clip(lower=0)
        out["free_after"] = (out["capacity"] - out["inventory_after"]).clip(lower=0)
    return out


@dataclasses.dataclass(frozen=True)
class Observations:
    """The full set of marginals derived from a flow journal.

    Every field is a pure function of the journal (and, for ``inventory``, of the
    initial inventory). Bundling them in one container means the historical and
    simulated observation sets are produced by the same code path and therefore
    coincide by construction: the base-replay invariant
    ``simulated_departures == historical_departures`` rests on a single
    definition rather than two hand-kept blocks.

    Attributes
    ----------
    inventory : pandas.DataFrame
        Per-period inventory; see :func:`get_inventory_df`.
    departures : pandas.DataFrame
        Outflow per period and source; see :func:`flows_to_departures`.
    arrivals : pandas.DataFrame
        Inflow per period and target; see :func:`flows_to_arrivals`.
    demand : pandas.DataFrame
        Realized user demand; equals ``departures`` in an exact replay (see
        :func:`flows_to_departures` and the note on demand limiting).
    od_matrix : pandas.DataFrame
        Origin-destination demand model; see :func:`flows_to_od_matrix`.
    """

    inventory: pd.DataFrame
    departures: pd.DataFrame
    arrivals: pd.DataFrame
    demand: pd.DataFrame
    od_matrix: pd.DataFrame


# ---------------------------------------------------------------------------
# Run invariants (pure functions of the journal)
# ---------------------------------------------------------------------------
# I1 and I2 of the loss-logging design: whole-journal properties, returned as a
# list of human-readable violations (empty == holds) rather than raised, so the
# simulator-layer ``validate_run`` can collect I1-I4 together and report once.
def check_demand_split(flows: pd.DataFrame, demand: pd.DataFrame) -> list[str]:
    """I1 -- demand splits exactly into served departures and stockout losses.

    Per ``(period_id, facility_id, commodity_category)`` the input demand must
    equal ``Σ departed + Σ lost(reason="stockout")``. Checkable because ``demand``
    is a scenario input, not derived from the journal. Never triggers in an exact replay
    (no stockout, so ``departed == demand``).
    """
    keys = ["period_id", "facility_id", "commodity_category"]
    demand = demand.astype(
        {
            "period_id": "Int64",
            "facility_id": "string",
            "commodity_category": "string",
            "quantity": "Int64",
        }
    ).rename(columns={"quantity": "demand"})
    served = flows_to_departures(flows).rename(columns={"quantity": "departed"})
    lost = flows[(flows["event_type"] == "lost") & (flows["reason"] == "stockout")]
    lost_keys = ["period_id", "source_id", "commodity_category"]
    lost_demand = (
        lost.groupby(lost_keys, as_index=False)["quantity"]
        .sum()
        .rename(columns={"source_id": "facility_id", "quantity": "lost_demand"})
    )
    merged = (
        demand.merge(served, on=keys, how="outer")
        .merge(lost_demand, on=keys, how="outer")
        .fillna(0)
    )
    bad = merged[merged["demand"] != merged["departed"] + merged["lost_demand"]]
    return [
        f"I1 {r.facility_id}/{r.commodity_category} p{r.period_id}: demand={int(r.demand)} "
        f"!= departed={int(r.departed)} + lost_demand={int(r.lost_demand)}"
        for r in bad.itertuples(index=False)
    ]


def check_flow_closure(flows: pd.DataFrame) -> list[str]:
    """I2 -- every flow due by the horizon closes with exactly one terminal event.

    A flow's lifecycle opens with its move-0 ``departed`` and closes with exactly
    one terminal: an ``arrived`` (the bike docked, possibly after a redirect) or a
    ``lost`` with ``reason == "dock_full"`` (it found no free dock anywhere). A
    ``redirected`` is no longer terminal -- it is the intermediate bounce off a
    full station, and the flow goes on to a second arc that ends in ``arrived``. A
    flow whose ``planned_end_period`` falls past the last period of the run is
    legitimately still in transit -- the run window ended mid-trip -- so only
    flows due by the horizon are required to have closed. More than one terminal
    is always a double close. Stockout losses carry no ``flow_id`` and have no
    lifecycle of their own, so they are excluded.

    The count is per ``flow_id``: a redirect flow has two ``departed`` rows (move
    0 and move 1), so the lifecycle is opened by the move-0 ``departed`` only and
    closed once (the move-1 ``arrived``).
    """
    if flows.empty:
        return []
    last_period = int(flows["period_id"].max())
    # One row per flow: the move-0 departure opens the lifecycle, and its
    # planned_end_period is when the whole trip was first due to dock.
    opened = flows.loc[
        (flows["event_type"] == "departed") & (flows["move_id"] == 0),
        ["flow_id", "planned_end_period"],
    ]
    is_terminal = (flows["event_type"] == "arrived") | (
        (flows["event_type"] == "lost") & (flows["reason"] == "dock_full")
    )
    terminal = flows[is_terminal & flows["flow_id"].notna()]
    closes = terminal.groupby("flow_id").size()
    opened = opened.assign(n=opened["flow_id"].map(closes).fillna(0).astype("int64"))
    violations = []
    due = opened[opened["planned_end_period"] <= last_period]
    stuck = int((due["n"] == 0).sum())
    if stuck:
        violations.append(f"I2 flow closure: {stuck} flows due by the horizon never closed")
    doubled = int((opened["n"] > 1).sum())
    if doubled:
        violations.append(f"I2 flow closure: {doubled} flows have multiple terminal events")
    return violations
