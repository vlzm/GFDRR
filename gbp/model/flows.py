"""Flow journal: the event schema, builders, log, and its read-models.

Its read-models are the marginal observations. The flow journal is the single
source of truth for what happened in a run. This
module owns one secret -- the shape of a flow event -- on both sides: the
builders that *write* events (:func:`departed_events`, :func:`arrived_events`,
:func:`redirected_events`, :func:`redirect_leg_events`,
:func:`lost_events`) and the derivations that *read*
the journal back into
marginals (:func:`flows_to_departures` and friends). Write and
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

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:  # gbp.routing imports from gbp.model, so runtime import would cycle
    from gbp.routing import Routes

# ---------------------------------------------------------------------------
# Flow-event schema
# ---------------------------------------------------------------------------
# Two ids place each event inside its trip (Notations.md §0): ``move_id`` is the
# arc index (0..m -- one physical edge; each redirect bounce adds one more arc)
# and ``event_id`` is the event ordinal (0..n). Arc ``m`` opens with ``departed``
# at event ``2m`` and ends at event ``2m + 1`` (``arrived``, ``redirected`` or
# ``lost``). Both ids are set by the builders at emit time, so the live and
# finalized journals carry them alike. Row uniqueness is ``(flow_id, event_id)``.
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
# (the historical loader) stamps it through :func:`stamp_history_ordering`,
# which applies :func:`phase_rank_by_timing`.
DOCK_PREVIOUS_RANK = 0  # dock bikes that left in an earlier period
PERIOD_OWN_RANK = 1  # this period's own departures and stockout losses
DOCK_SAME_RANK = 2  # dock bikes that left and arrive within this same period
REBALANCE_RANK = 3  # the period's truck pickups and dropoffs (Notations.md §14)

# Event types that dock a bike (+1 at ``realized_target_id``). Only an
# ``arrived`` lands a bike -- including the ``arrived`` that ends a redirect's
# last arc, whose ``realized_target_id`` is the station the bike finally
# reached. ``redirected`` is the intermediate *bounce* off a full station and
# docks nothing (see :func:`redirected_events`); ``lost`` docks nowhere and a
# ``departed`` undocks (-1 at ``source_id``), so neither belongs here.
DOCKING_EVENT_TYPES = ["arrived"]


# ---------------------------------------------------------------------------
# The event predicates every inventory reader shares
# ---------------------------------------------------------------------------
# Two masks are the single definition of "which events move inventory"
# (Notations.md §0): an undocking is the -1 side, a docking is the +1 side.
# A third mask, ``is_user_departure``, narrows the undockings to the demand
# side (user trips only) for the read-models that mean "what riders wanted".
def is_undocking(flows: pd.DataFrame) -> pd.Series:
    """Mask of undocking events: a ``departed`` that takes a bike out of a dock.

    The ``-1`` side of the inventory rule (at ``source_id``), the mirror of
    :func:`is_docking`. An opening ``departed`` (``move_id == 0``) undocks a
    bike whether a user rides it (``flow_type == "user_trip"``) or a truck
    carries it away (``flow_type == "rebalance"``, Notations.md §14). A
    redirect's continuation leg is also a ``departed`` but with
    ``move_id >= 1`` -- pure transport that moves no inventory -- so it is
    excluded.
    """
    return (flows["event_type"] == "departed") & (flows["move_id"] == 0)


def is_user_departure(flows: pd.DataFrame) -> pd.Series:
    """Mask of real user departures: an undocking ``departed`` of a user trip.

    The demand side: these events are the outflow and the trips the OD model
    learns from. A rebalance pickup also undocks a bike (see
    :func:`is_undocking`) but is a truck move, not demand, so it is excluded
    here.
    """
    return is_undocking(flows) & (flows["flow_type"] == "user_trip")


def is_docking(flows: pd.DataFrame) -> pd.Series:
    """Mask of docking events: an ``arrived`` lands a bike at ``realized_target_id``.

    The ``+1`` side of the inventory rule (see :data:`DOCKING_EVENT_TYPES`).
    A ``redirected`` bounce and a ``lost`` dock nothing.
    """
    return flows["event_type"].isin(DOCKING_EVENT_TYPES)


def _event_deltas(events: pd.DataFrame, extra_cols: tuple[str, ...] = ()) -> pd.DataFrame:
    """One signed ``delta`` per inventory-moving event, at the facility it touches.

    The single ``+1`` / ``-1`` rule, written once: a docking (:func:`is_docking`)
    is ``+quantity`` at its ``realized_target_id`` and an undocking
    (:func:`is_undocking`) is ``-quantity`` at its ``source_id``; every other
    event moves no inventory and is dropped. ``extra_cols`` names event columns
    to carry along (the read-models keep ``step_id`` and ``period_id`` to
    cumulate on a time axis; a write-time batch needs none).
    """
    cols = [*extra_cols, "facility_id", "commodity_category", "delta"]
    dock = events[is_docking(events)].copy()
    dock["facility_id"] = dock["realized_target_id"]
    dock["delta"] = dock["quantity"].astype("int64")
    undock = events[is_undocking(events)].copy()
    undock["facility_id"] = undock["source_id"]
    undock["delta"] = -undock["quantity"].astype("int64")
    return pd.concat([dock[cols], undock[cols]], ignore_index=True)


def inventory_deltas_from_events(events: pd.DataFrame) -> pd.DataFrame:
    """Net inventory change one batch of events implies, per (facility, commodity).

    The write-time form of the one ``+1`` / ``-1`` rule (:func:`_event_deltas`),
    summed per (facility, commodity). ``SimulationState.apply_step_events`` uses
    it to move the live inventory by exactly what the events it writes imply;
    the read-models cumulate the same per-event deltas back from the journal,
    so the live inventory and the journal cannot state the rule differently.

    Parameters
    ----------
    events : pandas.DataFrame
        Flow-event rows (builder output or a journal slice).

    Returns
    -------
    pandas.DataFrame
        ``facility_id``, ``commodity_category``, ``delta`` -- one row per
        (facility, commodity) the batch touches; empty when nothing moves.
    """
    return (
        _event_deltas(events)
        .groupby(["facility_id", "commodity_category"], as_index=False)["delta"]
        .sum()
    )


# ---------------------------------------------------------------------------
# Flow-event builders
# ---------------------------------------------------------------------------
def _typed_events(events_df: pd.DataFrame) -> pd.DataFrame:
    """Cast event columns to the canonical dtypes so frames concat cleanly.

    The phase-ordering columns -- ``phase_rank``, ``phase_round`` and
    ``step_id`` -- are not set by the builders, so they are cast only when already
    present (an empty journal carries them; a freshly built event batch does not).
    In the simulator all three are stamped when a phase writes the events
    (``SimulationState.apply_step_events``); the historical loader stamps all
    three with :func:`stamp_history_ordering`.
    """
    for col, dtype in FLOW_EVENT_DTYPES.items():
        if col in events_df.columns:
            events_df[col] = events_df[col].astype(dtype)
    return events_df


def departed_events(trips: pd.DataFrame) -> pd.DataFrame:
    """One ``departed`` event row per trip leaving this period (move 0, event 0).

    This is the trip's opening event: a real user departure from a dock. The
    departure of a redirect's later leg is built by :func:`redirect_leg_events`
    instead and carries ``move_id >= 1``, so readers that count user departures
    filter on ``move_id == 0``.
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
    """One ``arrived`` event row per in-transit flow docking at ``period_id``.

    Ends the flow's current arc: for arc ``m`` (the rows' ``move_id``; 0 when
    the column is absent) the arrival is event ``2m + 1``. Arc 0 is a normal
    trip docking at its planned target; a higher arc is a redirect leg docking
    at the station the redirect chose.

    ``period_id`` is the docking period: a single int when a whole batch docks
    in the same period (the simulator), or a per-row Series of end periods when
    each flow docks at its own time (the historical log).
    """
    move = in_transit_due["move_id"] if "move_id" in in_transit_due.columns else 0
    return _typed_events(
        pd.DataFrame(
            {
                "flow_id": in_transit_due["flow_id"],
                "move_id": move,
                "event_id": 2 * move + 1,
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
    """One ``redirected`` *bounce* event per overflow flow (arc ``m``, event ``2m + 1``).

    Closes the flow's current arc: the bike reached the arc's target but the
    docks were full, so it bounced and did *not* dock there
    (``realized_target_id`` is NA). The bike rides on: :func:`redirect_leg_events`
    opens the next arc to the station chosen for it.

    Parameters
    ----------
    flows : pandas.DataFrame
        The overflow flows that were redirected, carrying ``source_id``,
        ``planned_target_id``, ``commodity_category``, ``start_period``,
        ``planned_end_period`` and the current arc's ``move_id`` (0 when the
        column is absent).
    period_id : int
        The period the bounce happened in (the current arc ends here).

    Returns
    -------
    pandas.DataFrame
        One ``redirected`` flow-event row per flow.
    """
    move = flows["move_id"] if "move_id" in flows.columns else 0
    return _typed_events(
        pd.DataFrame(
            {
                "flow_id": flows["flow_id"],
                "move_id": move,
                "event_id": 2 * move + 1,
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


def redirect_leg_events(redirects: pd.DataFrame, period_id: int) -> pd.DataFrame:
    """One ``departed`` row per redirected flow: the new leg after a bounce.

    After the arc-``m`` bounce (:func:`redirected_events`) the bike rides a new
    arc ``m + 1`` (event ``2m + 2``) from the full station it bounced off to the
    station chosen for it, due to dock at ``leg_end_period``. The leg is pure
    transport: the bike never held a dock at the full station, so this
    ``departed`` moves no inventory and is not a user departure (readers filter
    ``move_id == 0``). ``start_period`` stays the flow's opening period on every
    row. The rows also serve as the in-transit entries for legs that take time;
    their arrival is built by :func:`arrived_events` when they dock.

    Parameters
    ----------
    redirects : pandas.DataFrame
        Overflow flows with a planned leg, carrying ``flow_id``, ``move_id``
        (the arc that just bounced), ``commodity_category``,
        ``planned_target_id`` (the full station the leg departs),
        ``realized_target_id`` (the station it heads to), ``start_period`` and
        ``leg_end_period`` (the period it will dock).
    period_id : int
        The bounce period; the leg departs here.

    Returns
    -------
    pandas.DataFrame
        One ``departed`` flow-event row per redirected flow.
    """
    move = redirects["move_id"] + 1
    return _typed_events(
        pd.DataFrame(
            {
                "flow_id": redirects["flow_id"],
                "move_id": move,
                "event_id": 2 * move,
                "period_id": period_id,
                "flow_type": "user_trip",
                "event_type": "departed",
                "commodity_category": redirects["commodity_category"],
                "source_id": redirects["planned_target_id"],
                "planned_target_id": redirects["realized_target_id"],
                "realized_target_id": pd.NA,
                "start_period": redirects["start_period"],
                "planned_end_period": redirects["leg_end_period"],
                "realized_end_period": pd.NA,
                "resource_id": pd.NA,
                "quantity": 1,
                "reason": pd.NA,
            }
        )
    )


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
    # it); a dock-full loss closes the flow's current arc ``m`` (event ``2m + 1``,
    # where ``m`` is the losses' ``move_id`` -- 0 for a plain trip, higher for a
    # redirect leg that found no dock anywhere).
    if reason == "stockout":
        move: int | pd.Series = 0
    else:
        move = losses["move_id"] if "move_id" in losses.columns else 0
    event_id = 0 if reason == "stockout" else 2 * move + 1
    na = pd.Series([pd.NA] * len(losses), index=losses.index)
    return _typed_events(
        pd.DataFrame(
            {
                "flow_id": losses.get("flow_id", na),
                "move_id": move,
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


def rebalance_departed_events(pickups: pd.DataFrame) -> pd.DataFrame:
    """One ``departed`` row per bike a truck picks up (``flow_type="rebalance"``).

    Opens a rebalance flow (Notations.md §14): the truck takes the bike out of
    a dock at ``source_id`` (``-1`` there, see :func:`is_undocking`) and will
    carry it to ``planned_target_id``. The rows also serve as the in-transit
    entries while the bike rides on the truck; :func:`rebalance_arrived_events`
    closes them when the truck drops the bike off.

    Parameters
    ----------
    pickups : pandas.DataFrame
        One row per picked-up bike, carrying ``flow_id``, ``source_id``,
        ``planned_target_id``, ``commodity_category``, ``resource_id`` (the
        truck), ``start_period`` (the pickup period) and ``planned_end_period``
        (the planned dropoff period).

    Returns
    -------
    pandas.DataFrame
        One ``departed`` flow-event row per bike (move 0, event 0).
    """
    return _typed_events(
        pd.DataFrame(
            {
                "flow_id": pickups["flow_id"],
                "move_id": 0,
                "event_id": 0,
                "period_id": pickups["start_period"],
                "flow_type": "rebalance",
                "event_type": "departed",
                "commodity_category": pickups["commodity_category"],
                "source_id": pickups["source_id"],
                "planned_target_id": pickups["planned_target_id"],
                "realized_target_id": pd.NA,
                "start_period": pickups["start_period"],
                "planned_end_period": pickups["planned_end_period"],
                "realized_end_period": pd.NA,
                "resource_id": pickups["resource_id"],
                "quantity": 1,
                "reason": pd.NA,
            }
        )
    )


def rebalance_arrived_events(dropoffs: pd.DataFrame, period_id: int) -> pd.DataFrame:
    """One ``arrived`` row per rebalance flow whose truck drops it off at ``period_id``.

    Closes the rebalance flow (event 1): the bike docks at the row's
    ``realized_target_id`` (``+1`` there, see :func:`is_docking`). The calling
    phase sets ``realized_target_id`` before building the events: the planned
    station when its docks have room, the truck's home depot when they do not
    -- the planned-vs-realized split (Notations.md §5) records the difference.

    Parameters
    ----------
    dropoffs : pandas.DataFrame
        The in-transit rebalance rows being dropped off, carrying ``flow_id``,
        ``source_id``, ``planned_target_id``, ``realized_target_id`` (where the
        bike actually docks), ``commodity_category``, ``resource_id``,
        ``start_period`` and ``planned_end_period``.
    period_id : int
        The period the dropoff happens in.

    Returns
    -------
    pandas.DataFrame
        One ``arrived`` flow-event row per bike (move 0, event 1).
    """
    return _typed_events(
        pd.DataFrame(
            {
                "flow_id": dropoffs["flow_id"],
                "move_id": 0,
                "event_id": 1,
                "period_id": period_id,
                "flow_type": "rebalance",
                "event_type": "arrived",
                "commodity_category": dropoffs["commodity_category"],
                "source_id": dropoffs["source_id"],
                "planned_target_id": dropoffs["planned_target_id"],
                "realized_target_id": dropoffs["realized_target_id"],
                "start_period": dropoffs["start_period"],
                "planned_end_period": dropoffs["planned_end_period"],
                "realized_end_period": period_id,
                "resource_id": dropoffs["resource_id"],
                "quantity": 1,
                "reason": pd.NA,
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


#: Ordering columns stamped at write time (``SimulationState.apply_step_events``).
#: In-transit rows are kept in builder shape, so these are dropped on entry.
_ORDERING_COLUMNS = ["phase_rank", "phase_round", "step_id"]


def in_transit_after_events(in_transit: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    """Return the in-transit set after one batch of events.

    A ``departed`` row opens an arc: the bike is riding (or on a truck) and the
    row itself is the in-transit entry. The event that ends the same arc --
    ``arrived``, ``redirected`` or ``lost`` with the same ``(flow_id,
    move_id)`` -- closes it and removes the entry. An arc opened and closed
    inside the same batch (a zero-duration redirect leg, a same-period truck
    dropoff) never enters the set. A stockout ``lost`` has no ``flow_id`` and
    closes nothing.

    Parameters
    ----------
    in_transit : pandas.DataFrame
        The current in-transit set (``departed``-event rows).
    events : pandas.DataFrame
        One batch of flow-event rows.

    Returns
    -------
    pandas.DataFrame
        The in-transit set after the batch.
    """
    closing = events[events["event_type"].isin(["arrived", "redirected", "lost"])]
    closing = closing[closing["flow_id"].notna()]
    closed = pd.MultiIndex.from_frame(closing[["flow_id", "move_id"]])
    opened = events[events["event_type"] == "departed"]
    riding = opened[~pd.MultiIndex.from_frame(opened[["flow_id", "move_id"]]).isin(closed)]
    riding = riding.drop(columns=[c for c in _ORDERING_COLUMNS if c in riding.columns])
    kept = in_transit[~pd.MultiIndex.from_frame(in_transit[["flow_id", "move_id"]]).isin(closed)]
    return pd.concat([kept, riding], ignore_index=True)


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
    with this rule (through :func:`stamp_history_ordering`) rather than knowing it
    from a phase. The simulator, which runs real phases, stamps its phase's rank
    directly and never calls this.

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
    ``test_stamped_step_id_matches_tuple_order`` uses this function as the
    independent oracle that locks that agreement.
    """
    is_departure = is_user_departure(flows)
    is_stockout = (flows["event_type"] == "lost") & (flows["reason"] == "stockout")
    is_period_own = is_departure | is_stockout
    rank = pd.Series(DOCK_PREVIOUS_RANK, index=flows.index, dtype="int64")
    rank = rank.mask(is_period_own, PERIOD_OWN_RANK)
    rank = rank.mask(~is_period_own & (flows["period_id"] == flows["start_period"]), DOCK_SAME_RANK)
    return rank


def stamp_history_ordering(journal: pd.DataFrame) -> pd.DataFrame:
    """Stamp the three ordering columns on a journal built without phases.

    The one producer with no phases is the historical loader: it turns raw
    trips into a journal in one pass, so no phase ran and nothing opened
    inventory steps. This function stamps what the write path would have
    stamped (Notations.md §0.1):

    - ``phase_rank`` -- by the timing rule (:func:`phase_rank_by_timing`);
    - ``phase_round`` -- 0 on every row: a source with no phases has no rounds;
    - ``step_id`` -- the distinct ``(period_id, phase_rank, phase_round)``
      labels numbered 0, 1, 2, ... in sorted order. Safe here because history
      is pure user trips -- no redirects, no rebalancing -- so one label is
      always exactly one inventory batch.

    The simulator never calls this: its ``step_id`` is handed out by the
    run-global counter at write time (``SimulationState.apply_step_events``)
    and is never derived from the event columns. That keeps one answer per
    producer to "where does an event's ``step_id`` come from".
    """
    flows = journal.copy()
    flows["phase_rank"] = phase_rank_by_timing(flows)
    flows["phase_round"] = pd.Series(0, index=flows.index, dtype="int64")
    # ngroup with sort=True numbers the distinct labels in sorted (= step) order.
    keys = ["period_id", "phase_rank", "phase_round"]
    flows["step_id"] = flows.groupby(keys, sort=True).ngroup()
    return flows


def finalize_flows(journal: pd.DataFrame) -> pd.DataFrame:
    """Order the accumulated journal by its stamped steps and project the columns.

    Both the historical log (:func:`dataloader_graph.get_historical_flows_df`)
    and a replay run's journal are finalized through this one function, so they
    share their order and their column projection by construction rather than by
    two definitions kept in sync by hand. It assigns nothing. The trip ids
    (``move_id``, ``event_id``) are set by the builders at emit time. The
    ordering columns (``phase_rank``, ``phase_round``, ``step_id``) are stamped
    before the journal gets here: by ``SimulationState.apply_step_events`` in
    the simulator, or by :func:`stamp_history_ordering` in the historical
    loader. A journal with a hole in those columns is refused -- filling it
    here would be a second definition of ``step_id``.

    Parameters
    ----------
    journal : pandas.DataFrame
        The append-only journal accumulated during a run, with the ordering
        columns stamped on every row.

    Returns
    -------
    pandas.DataFrame
        Event log with columns :data:`FLOW_EVENT_COLUMNS`, ordered by ``step_id``
        then ``flow_id`` then ``event_id``. Within a flow the ``event_id`` order
        matches the events' time order, so each trip's events stay in sequence.

    Raises
    ------
    ValueError
        If an ordering column is absent or NA on any row.
    """
    flows = journal.copy()
    for col, dtype in FLOW_EVENT_DTYPES.items():
        if col in flows.columns:
            flows[col] = flows[col].astype(dtype)
    for col in _ORDERING_COLUMNS:
        if col not in flows.columns or flows[col].isna().any():
            raise ValueError(
                f"finalize_flows: journal rows arrived without {col!r}. The simulator "
                "stamps the ordering columns in SimulationState.apply_step_events; a "
                "source with no phases stamps them with stamp_history_ordering."
            )
    flows = flows.sort_values(["step_id", "flow_id", "event_id"], kind="stable")
    return flows.reset_index(drop=True)[FLOW_EVENT_COLUMNS]


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
    departed = flows[is_user_departure(flows)]
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
    docked = flows[is_docking(flows)]
    keys = ["period_id", "realized_target_id", "commodity_category"]
    return (
        docked.groupby(keys, as_index=False)["quantity"]
        .sum()
        .rename(columns={"realized_target_id": "facility_id"})
    )


def flows_to_redirects(flows: pd.DataFrame) -> pd.DataFrame:
    """Bounces per period and facility: ``redirected`` events at the full target.

    A ``redirected`` event is the bounce off the full ``planned_target_id`` of
    the current arc, so the bounce is counted at that facility. Grouped by
    ``(period_id, facility_id, commodity_category)``.
    """
    bounced = flows[flows["event_type"] == "redirected"]
    return (
        bounced.groupby(["period_id", "planned_target_id", "commodity_category"], as_index=False)[
            "quantity"
        ]
        .sum()
        .rename(columns={"planned_target_id": "facility_id"})
    )


def flows_to_losses(flows: pd.DataFrame, reason: str) -> pd.DataFrame:
    """Losses per period and facility for one loss reason.

    The reason decides where the loss lands. A ``stockout`` loss lands at the
    trip's ``source_id`` (the dock had no bike to give). A ``dock_full`` loss
    lands at the trip's ``planned_target_id`` (the bike found no free dock
    anywhere). Grouped by ``(period_id, facility_id, commodity_category)``.
    """
    facility_col = {"stockout": "source_id", "dock_full": "planned_target_id"}[reason]
    lost = flows[(flows["event_type"] == "lost") & (flows["reason"] == reason)]
    return (
        lost.groupby(["period_id", facility_col, "commodity_category"], as_index=False)["quantity"]
        .sum()
        .rename(columns={facility_col: "facility_id"})
    )


def flows_to_od_matrix(flows: pd.DataFrame) -> pd.DataFrame:
    """Build the OD demand model (probability and duration per source-target pair)."""
    # Only user departures are intended trips. A redirect's later-leg departure
    # is a forced transport leg and would pollute the demand model.
    dep = flows[is_user_departure(flows)].copy()
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
    ``realized_target_id`` (see :data:`DOCKING_EVENT_TYPES`) and an undocking
    ``departed`` (``move_id == 0``, a user departure or a rebalance pickup) is
    ``-1`` at its ``source_id``. A redirect's
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
        A finalized flow-event log (it must carry ``step_id``).
    initial_inventory : pandas.DataFrame
        Starting inventory with ``facility_id``, ``commodity_category``, ``quantity``.

    Returns
    -------
    pandas.DataFrame
        Columns ``period_id``, ``facility_id``, ``commodity_category``,
        ``quantity_sop`` (start-of-period inventory: what is on hand before the
        period's own flows, equal to the previous period's end value, and the
        initial inventory for period 0) and ``quantity_eop`` (end-of-period
        inventory: after the period's own flows). One row per
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

    # The per-period deltas are the per-step deltas aggregated by period, so the
    # coarse and the fine inventory views share one delta rule by construction.
    deltas = (
        _inventory_deltas(flows)
        .groupby(["period_id", "facility_id", "commodity_category"], as_index=False)["delta"]
        .sum()
    )

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
    # End-of-period inventory: initial plus the running total of net flow up to
    # and including each period. Start-of-period inventory is that minus the
    # period's own net flow, so it equals the previous period's end value (and
    # the initial inventory for period 0).
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


#: The panel's row key and value columns, in canonical order.
PANEL_KEYS = ["period_id", "facility_id", "commodity_category"]
PANEL_VALUES = [
    "quantity_sop",
    "quantity_eop",
    "demand",
    "departed",
    "arrived",
    "redirected",
    "lost_demand",
    "lost_dock_full",
]


def flows_to_panel(flows: pd.DataFrame, initial_inventory: pd.DataFrame) -> pd.DataFrame:
    """Build the per-(period, facility, commodity) state of the run, in one table.

    One row per ``(period_id, facility_id, commodity_category)`` -- covering
    every period up to the journal's last, even pairs with no events -- with
    the period's values side by side:

    - ``quantity_sop`` / ``quantity_eop`` -- start / end inventory
      (:func:`get_inventory_df`);
    - ``departed``, ``arrived``, ``redirected`` -- the event marginals
      (:func:`flows_to_departures`, :func:`flows_to_arrivals`,
      :func:`flows_to_redirects`);
    - ``lost_demand`` -- stockout losses, landed at the trip's ``source_id``;
    - ``lost_dock_full`` -- dock-full losses, landed at the trip's
      ``planned_target_id`` (see :func:`flows_to_losses` for why the two
      reasons land at different facilities);
    - ``demand = departed + lost_demand`` -- the demand identity
      (Notations.md §2; checked run-wide by :func:`check_demand_split`).

    This read-model owns two guarantees the callers would otherwise restate:
    the period grid covers every event (an event at a (facility, commodity)
    pair the inventory grid does not know raises ``ValueError``), and the
    demand identity holds per row by construction.

    Parameters
    ----------
    flows : pandas.DataFrame
        A finalized flow-event log (it must carry ``step_id``).
    initial_inventory : pandas.DataFrame
        Starting inventory: ``facility_id``, ``commodity_category``, ``quantity``.

    Returns
    -------
    pandas.DataFrame
        The panel, keyed by :data:`PANEL_KEYS` with :data:`PANEL_VALUES`
        columns.
    """
    panel = get_inventory_df(flows, initial_inventory)

    counts: dict[str, pd.DataFrame] = {
        "departed": flows_to_departures(flows),
        "arrived": flows_to_arrivals(flows),
        "redirected": flows_to_redirects(flows),
        "lost_demand": flows_to_losses(flows, "stockout"),
        "lost_dock_full": flows_to_losses(flows, "dock_full"),
    }
    for name, grouped in counts.items():
        grouped = grouped.rename(columns={"quantity": name})
        panel = panel.merge(grouped, on=PANEL_KEYS, how="left")
        panel[name] = panel[name].fillna(0).astype("int64")
        # The inventory grid must cover every event; a mismatch means an event
        # happened at a (facility, commodity) pair the grid does not know.
        if int(panel[name].sum()) != int(grouped[name].sum()):
            raise ValueError(f"panel dropped {name} events outside the inventory grid")

    panel["demand"] = panel["departed"] + panel["lost_demand"]
    return panel[PANEL_KEYS + PANEL_VALUES]


def _inventory_deltas(flows: pd.DataFrame) -> pd.DataFrame:
    """Per-event deltas with ``step_id`` and ``period_id`` kept for cumulation.

    The read-time view of the one ``+1`` / ``-1`` rule (:func:`_event_deltas`):
    each row keeps its ``step_id`` and ``period_id`` so the deltas can be
    cumulated on either time axis -- per step (:func:`inventory_at_moments`) or
    per period (:func:`get_inventory_df`).
    """
    return _event_deltas(flows, ("step_id", "period_id"))


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
    ``(facility, commodity)`` it changes -- ``source_id`` for an undocking
    ``departed`` (the ``-1``), ``realized_target_id`` for a docking ``arrived``
    (the ``+1``).
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
    is_dock = is_docking(out)
    is_dep = is_undocking(out)
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


_ONE_HOUR = pd.Timedelta(hours=1)


def flows_with_costs(
    flows: pd.DataFrame,
    rates: pd.DataFrame,
    period_len: pd.Timedelta,
) -> pd.DataFrame:
    """Widen the journal with each event's riding time so far and the money it accrued.

    Every event row gains three columns:

    - ``rate`` -- the price of riding this commodity, in dollars per hour.
    - ``elapsed_periods`` -- how many periods the flow has been riding at this
      event: ``period_id - start_period``. ``start_period`` is the flow's
      opening period on every row, redirect legs included, so the value is
      cumulative over legs: 0 on the opening ``departed``, the first leg's
      length on a ``redirected`` bounce, the sum of all legs on the final
      ``arrived``.
    - ``cost`` -- dollars accrued so far: ``rate * elapsed_periods * hours per
      period``. Cumulative like ``elapsed_periods``; a trip's total cost is the
      value on its final ``arrived``.

    A stockout ``lost`` row has no ``start_period`` (the trip never departed),
    so its ``elapsed_periods`` and ``cost`` stay NA: nothing was ridden, nothing
    accrued. A dock-full ``lost`` row closes a real arc and keeps the time
    ridden up to the loss.

    Parameters
    ----------
    flows : pandas.DataFrame
        A flow-event log, historical or simulated.
    rates : pandas.DataFrame
        Per-commodity price: ``commodity_category``, ``rate`` (dollars per
        hour; ``commodities_categories_rates_df`` in the resolved data).
    period_len : pandas.Timedelta
        Wall-clock length of one period; converts periods to hours for the
        cost. Required on purpose: a forgotten period length would price the
        run wrong without any error.

    Returns
    -------
    pandas.DataFrame
        ``flows`` plus ``rate``, ``elapsed_periods`` and ``cost``.
    """
    # The rates table may carry a plain object key and an integer rate; cast
    # both so the merge keeps the journal's string dtype on commodity_category
    # and the widened journal always prices in float dollars.
    rates_typed = rates[["commodity_category", "rate"]].astype(
        {"commodity_category": "string", "rate": "float64"}
    )
    out = flows.merge(rates_typed, on="commodity_category", how="left")
    out["elapsed_periods"] = (out["period_id"] - out["start_period"]).astype("Int64")
    hours_per_period = period_len / _ONE_HOUR
    out["cost"] = out["rate"] * out["elapsed_periods"] * hours_per_period
    return out


def flows_with_measures(
    flows: pd.DataFrame,
    *,
    routes: "Routes",
    rates: pd.DataFrame,
    period_len: pd.Timedelta,
) -> pd.DataFrame:
    """Widen the journal with the measures: duration, distance and money columns.

    This is the one place that widens a journal with per-event measures
    (Notations.md §6.1); the canonical notebook and the artifact builder both
    call it. Every event row gains:

    - ``planned_duration_periods`` (``planned_end_period - start_period``) and
      ``realized_duration_periods`` (``realized_end_period - start_period``,
      NA if lost) -- the trip length in whole periods, planned vs realized.
    - ``planned_distance_km`` (source to planned target) and
      ``realized_distance_km`` (source to realized target) -- measured by the
      scenario's routing mode.
    - ``rate``, ``elapsed_periods``, ``cost`` -- the event's riding time so far
      and the money it accrued (see :func:`flows_with_costs`).

    Parameters
    ----------
    flows : pandas.DataFrame
        A flow-event log, historical or simulated.
    routes : gbp.routing.Routes
        The scenario's distance / travel-time answerer.
    rates : pandas.DataFrame
        Per-commodity price: ``commodity_category``, ``rate`` (dollars per hour).
    period_len : pandas.Timedelta
        Wall-clock length of one period (prices periods into dollars).

    Returns
    -------
    pandas.DataFrame
        ``flows`` plus the seven measure columns.
    """
    out = flows.copy()
    out["planned_duration_periods"] = out["planned_end_period"] - out["start_period"]
    out["realized_duration_periods"] = out["realized_end_period"] - out["start_period"]
    out["planned_distance_km"] = routes.distance_km(out["source_id"], out["planned_target_id"])
    out["realized_distance_km"] = routes.distance_km(out["source_id"], out["realized_target_id"])
    return flows_with_costs(out, rates, period_len)


_EARTH_RADIUS_KM = 6371.0088


def haversine_km(lat1: pd.Series, lng1: pd.Series, lat2: pd.Series, lng2: pd.Series) -> pd.Series:
    """Great-circle distance in kilometres between two coordinate columns.

    Vectorised over the rows. Any row with a missing coordinate yields ``NaN``.
    It lives here in the model layer because both the loaders (trip distances,
    the wide journal) and the simulator (a redirect leg's travel-time estimate)
    need the same distance, and both may import from the model but never the
    other way around.
    """
    lat1_r, lng1_r, lat2_r, lng2_r = (np.radians(x) for x in (lat1, lng1, lat2, lng2))
    dlat = lat2_r - lat1_r
    dlng = lng2_r - lng1_r
    h = np.sin(dlat / 2) ** 2 + np.cos(lat1_r) * np.cos(lat2_r) * np.sin(dlng / 2) ** 2
    return _EARTH_RADIUS_KM * 2 * np.arcsin(np.sqrt(h))


def neighbor_distance_sq(
    lat: pd.Series,
    lng: pd.Series,
    other_lat: pd.Series,
    other_lng: pd.Series,
) -> pd.Series:
    """Rank neighbours by the one metric: squared Euclidean distance on (lat, lng).

    Any argument may also be a single float; pandas broadcasts it over the rows.

    Both the redirect mechanics (``_nearest_free_station``, which *decides*
    where a redirected bike goes) and the redirect explainer
    (:func:`redirect_neighbor_table`, which *explains* that decision afterwards)
    rank neighbour stations with this function, so the order the explainer shows
    is always the order the simulator used. Squared distance keeps the ranking
    exact without a square root. It lives here in the model layer because the
    mechanics may import from the model but never the other way around.
    """
    return (lat - other_lat) ** 2 + (lng - other_lng) ** 2


def _squared_distances(geo: pd.DataFrame, facility_id: str) -> pd.Series:
    """Squared distance from ``facility_id`` to every other facility, nearest first.

    Ranks with :func:`neighbor_distance_sq` -- the same metric and stable
    tie-break the redirect mechanics use -- so the order here matches the order
    a redirect actually walks. The facility itself is dropped.
    """
    coords = geo.set_index("facility_id")[["lat", "lng"]]
    o = coords.loc[facility_id]
    d2 = neighbor_distance_sq(coords["lat"], coords["lng"], o["lat"], o["lng"])
    return d2.drop(index=facility_id).sort_values(kind="stable")


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

    A redirected bike bounced off its full planned station ``B`` and was sent to
    a farther station ``C`` (a flow that bounced more than once is explained at
    its first bounce). To check that choice was right, you want to see, *at the
    redirect's moment*, every station between ``B`` and ``C`` in distance
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
    bounces = one[one["event_type"] == "redirected"]
    if bounces.empty:
        raise ValueError(f"flow {flow_id!r} has no redirect (no 'redirected' event)")
    bounce = bounces.iloc[0]  # a flow that bounced more than once: explain its first bounce
    full_station = bounce["planned_target_id"]  # B: the full station it bounced off
    step_id = int(bounce["step_id"])
    period_id = int(bounce["period_id"])
    commodity = bounce["commodity_category"]
    # C: the station this bounce's new leg heads to (the leg's planned target).
    leg = one[(one["event_type"] == "departed") & (one["move_id"] == bounce["move_id"] + 1)]
    realized_target = leg["planned_target_id"].iloc[0] if not leg.empty else pd.NA

    # Neighbours of B by distance, cut at C (inclusive) or the first n_neighbors.
    distances = _squared_distances(geo, full_station)
    order = list(distances.index)
    if n_neighbors is not None:
        order = order[:n_neighbors]
    elif pd.notna(realized_target) and realized_target in order:
        order = order[: order.index(realized_target) + 1]

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
            "realized_target_id": realized_target,
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


# ---------------------------------------------------------------------------
# Run invariants (pure functions of the journal)
# ---------------------------------------------------------------------------
# I1 and I2 of the loss-logging design: whole-journal properties, returned as a
# list of human-readable violations (empty == holds) rather than raised, so the
# simulator-layer ``validate_run`` can collect I1-I5 together and report once.
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
    lost_demand = flows_to_losses(flows, "stockout").rename(columns={"quantity": "lost_demand"})
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
    one terminal: an ``arrived`` (the bike docked, possibly after redirects) or a
    ``lost`` with ``reason == "dock_full"`` (it found no free dock anywhere). A
    ``redirected`` is not terminal -- it is the intermediate bounce off a full
    station, and the flow rides on on a new leg. A flow whose latest leg is due
    past the last period of the run is legitimately still in transit -- the run
    window ended mid-trip -- so only flows due by the horizon are required to
    have closed. More than one terminal is always a double close. Stockout
    losses carry no ``flow_id`` and have no lifecycle of their own, so they are
    excluded.
    """
    if flows.empty:
        return []
    last_period = int(flows["period_id"].max())
    # One row per flow. Each bounce opens a new leg with its own due period, so
    # a flow is due to close at its *latest* ``departed`` row's planned end.
    opened = (
        flows[flows["event_type"] == "departed"]
        .groupby("flow_id", as_index=False)["planned_end_period"]
        .max()
    )
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
