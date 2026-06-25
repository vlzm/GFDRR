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
    "flow_id", "move_id", "event_id", "period_id", "flow_type", "event_type",
    "commodity_category", "source_id", "planned_target_id", "realized_target_id",
    "start_period", "planned_end_period", "realized_end_period",
    "resource_id", "quantity", "reason",
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
}

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
    """Cast event columns to the canonical dtypes so frames concat cleanly."""
    for col, dtype in FLOW_EVENT_DTYPES.items():
        events_df[col] = events_df[col].astype(dtype)
    return events_df


def departed_events(trips: pd.DataFrame) -> pd.DataFrame:
    """One ``departed`` event row per trip leaving this period (move 0, event 0).

    This is the trip's opening event: a real user departure from a dock. The
    redirect's second-arc departure is built by
    :func:`redirect_continuation_events` instead and carries ``move_id == 1``, so
    readers that count user departures filter on ``move_id == 0``.
    """
    return _typed_events(pd.DataFrame({
        "flow_id":             trips["flow_id"],
        "move_id":             0,
        "event_id":            0,
        "period_id":           trips["start_period"],
        "flow_type":           "user_trip",
        "event_type":          "departed",
        "commodity_category":  trips["commodity_category"],
        "source_id":           trips["source_id"],
        "planned_target_id":   trips["planned_target_id"],
        "realized_target_id":  pd.NA,
        "start_period":        trips["start_period"],
        "planned_end_period":  trips["planned_end_period"],
        "realized_end_period": pd.NA,
        "resource_id":         pd.NA,
        "quantity":            1,
        "reason":              pd.NA,
    }))


def arrived_events(in_transit_due: pd.DataFrame, period_id: int | pd.Series) -> pd.DataFrame:
    """One ``arrived`` event row per in-transit flow docking at ``period_id`` (move 0, event 1).

    The normal-trip arrival: a bike docked at its planned target. (The arrival
    that ends a redirect's second arc is built by
    :func:`redirect_continuation_events`, with ``move_id == 1``.)

    ``period_id`` is the docking period: a single int when a whole batch docks
    in the same period (the simulator), or a per-row Series of end periods when
    each flow docks at its own time (the historical log).
    """
    return _typed_events(pd.DataFrame({
        "flow_id":             in_transit_due["flow_id"],
        "move_id":             0,
        "event_id":            1,
        "period_id":           period_id,
        "flow_type":           "user_trip",
        "event_type":          "arrived",
        "commodity_category":  in_transit_due["commodity_category"],
        "source_id":           in_transit_due["source_id"],
        "planned_target_id":   in_transit_due["planned_target_id"],
        "realized_target_id":  in_transit_due["planned_target_id"],
        "start_period":        in_transit_due["start_period"],
        "planned_end_period":  in_transit_due["planned_end_period"],
        "realized_end_period": period_id,
        "resource_id":         pd.NA,
        "quantity":            1,
        "reason":              pd.NA,
    }))


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
    return _typed_events(pd.DataFrame({
        "flow_id":             flows["flow_id"],
        "move_id":             0,
        "event_id":            1,
        "period_id":           period_id,
        "flow_type":           "user_trip",
        "event_type":          "redirected",
        "commodity_category":  flows["commodity_category"],
        "source_id":           flows["source_id"],
        "planned_target_id":   flows["planned_target_id"],
        "realized_target_id":  pd.NA,
        "start_period":        flows["start_period"],
        "planned_end_period":  flows["planned_end_period"],
        "realized_end_period": period_id,
        "resource_id":         pd.NA,
        "quantity":            1,
        "reason":              "dock_full",
    }))


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
        ``commodity_category``, ``planned_target_id`` (B, the full station) and
        ``realized_target_id`` (C, the station found for them).
    period_id : int
        The period the redirect happens in (departure from B and docking at C
        both fall here).

    Returns
    -------
    pandas.DataFrame
        Two rows per redirected flow: a move-1 ``departed`` then a move-1
        ``arrived``, ready to append to the journal.
    """
    full_station = redirected["planned_target_id"]   # B: source of the second leg
    docked_at = redirected["realized_target_id"]      # C: where the bike docks
    common = {
        "flow_id":             redirected["flow_id"],
        "period_id":           period_id,
        "flow_type":           "user_trip",
        "commodity_category":  redirected["commodity_category"],
        "source_id":           full_station,
        "planned_target_id":   docked_at,
        "start_period":        period_id,
        "planned_end_period":  period_id,
        "realized_end_period": period_id,
        "resource_id":         pd.NA,
        "quantity":            1,
        "reason":              pd.NA,
    }
    departed = _typed_events(pd.DataFrame({
        **common,
        "move_id":            1,
        "event_id":           2,
        "event_type":         "departed",
        "realized_target_id": pd.NA,
    }))
    arrived = _typed_events(pd.DataFrame({
        **common,
        "move_id":            1,
        "event_id":           3,
        "event_type":         "arrived",
        "realized_target_id": docked_at,
    }))
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
    return _typed_events(pd.DataFrame({
        "flow_id":             losses.get("flow_id", na),
        "move_id":             0,
        "event_id":            event_id,
        "period_id":           period_id,
        "flow_type":           "user_trip",
        "event_type":          "lost",
        "commodity_category":  losses["commodity_category"],
        "source_id":           losses["source_id"],
        "planned_target_id":   losses.get("planned_target_id", na),
        "realized_target_id":  pd.NA,
        "start_period":        losses.get("start_period", na),
        "planned_end_period":  losses.get("planned_end_period", na),
        "realized_end_period": pd.NA,
        "resource_id":         pd.NA,
        "quantity":            losses["quantity"],
        "reason":              reason,
    }))


def empty_in_transit() -> pd.DataFrame:
    """Empty in-transit table (a ``departed``-event frame with no rows)."""
    return departed_events(pd.DataFrame({
        "flow_id":            pd.Series(dtype="string"),
        "source_id":          pd.Series(dtype="string"),
        "planned_target_id":  pd.Series(dtype="string"),
        "commodity_category": pd.Series(dtype="string"),
        "start_period":       pd.Series(dtype="Int64"),
        "planned_end_period": pd.Series(dtype="Int64"),
    }))


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
    return pd.DataFrame(
        {col: pd.Series(dtype=dtype) for col, dtype in FLOW_EVENT_DTYPES.items()}
    )


def finalize_flows(journal: pd.DataFrame) -> pd.DataFrame:
    """Order the accumulated journal and project the canonical columns.

    Both the historical log (:func:`dataloader_graph.get_historical_flows_df`)
    and a replay run's journal are finalized through this one function, so they
    share their sort keys and column projection by construction rather than by
    two definitions kept in sync by hand. The ids (``move_id``, ``event_id``) are
    already set by the builders at emit time, so this assigns nothing -- it only
    sorts and projects.

    Parameters
    ----------
    journal : pandas.DataFrame
        The append-only journal accumulated during a run.

    Returns
    -------
    pandas.DataFrame
        Event log with columns :data:`FLOW_EVENT_COLUMNS`, sorted by
        ``period_id`` then ``flow_id`` then ``event_id``. Within a flow the
        ``event_id`` order matches the events' time order, so the sort lists each
        trip's events in sequence.
    """
    flows = journal.copy()
    for col, dtype in FLOW_EVENT_DTYPES.items():
        flows[col] = flows[col].astype(dtype)
    flows = flows.sort_values(["period_id", "flow_id", "event_id"]).reset_index(drop=True)
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
        departed
        .groupby(["period_id", "source_id", "commodity_category"], as_index=False)["quantity"].sum()
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
        docked.groupby(keys, as_index=False)["quantity"].sum()
        .rename(columns={"realized_target_id": "facility_id"})
    )


def flows_to_od_matrix(flows: pd.DataFrame) -> pd.DataFrame:
    # Only move-0 departures are intended trips. A redirect's move-1 departure is
    # a forced transport leg (duration 0) and would pollute the demand model.
    dep = flows[(flows["event_type"] == "departed") & (flows["move_id"] == 0)].copy()
    dep["duration"] = dep["planned_end_period"] - dep["start_period"]
    od = (
        dep.groupby(["source_id", "planned_target_id", "period_id", "commodity_category"], as_index=False)
        .agg(count=("quantity", "sum"), duration=("duration", "mean"))
    )
    totals = od.groupby(["source_id", "period_id","commodity_category"])["count"].transform("sum")
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
        ``quantity`` for every period in ``[0, max(period_id)]``.
    """
    if flows.empty:
        return pd.DataFrame({
            "period_id":          pd.Series(dtype="int64"),
            "facility_id":        pd.Series(dtype="string"),
            "commodity_category": pd.Series(dtype="string"),
            "quantity":           pd.Series(dtype="int64"),
        })

    dep = (
        flows[(flows["event_type"] == "departed") & (flows["move_id"] == 0)]
        .groupby(["period_id", "source_id", "commodity_category"], as_index=False)["quantity"].sum()
        .rename(columns={"source_id": "facility_id", "quantity": "delta"})
    )
    dep["delta"] = -dep["delta"]
    dock_keys = ["period_id", "realized_target_id", "commodity_category"]
    arr = (
        flows[flows["event_type"].isin(DOCKING_EVENT_TYPES)]
        .groupby(dock_keys, as_index=False)["quantity"].sum()
        .rename(columns={"realized_target_id": "facility_id", "quantity": "delta"})
    )
    deltas = pd.concat([dep, arr], ignore_index=True)
    deltas = deltas.groupby(
        ["period_id", "facility_id", "commodity_category"], as_index=False
    )["delta"].sum()

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
    cumulative = net.cumsum(axis=1).add(initial.reindex(full_index).fillna(0), axis=0)

    inventory = cumulative.stack().rename("quantity").reset_index()
    inventory["quantity"] = inventory["quantity"].astype("int64")
    return inventory[["period_id", "facility_id", "commodity_category", "quantity"]]


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
    demand = demand.astype({
        "period_id": "Int64", "facility_id": "string",
        "commodity_category": "string", "quantity": "Int64",
    }).rename(columns={"quantity": "demand"})
    served = flows_to_departures(flows).rename(columns={"quantity": "departed"})
    lost = flows[(flows["event_type"] == "lost") & (flows["reason"] == "stockout")]
    lost_keys = ["period_id", "source_id", "commodity_category"]
    lost_demand = (
        lost.groupby(lost_keys, as_index=False)["quantity"].sum()
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
