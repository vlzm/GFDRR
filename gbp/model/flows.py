"""Flow journal: the event schema, builders, log, and the read-models derived from it."""

import dataclasses
from collections.abc import Callable
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
# lower rank is applied first (Notations.md, "step / step_id"). The three
# user-trip ranks and the timing rule that recognizes them come from one ordered
# declaration -- :data:`CANONICAL_PHASE_ORDER` below, defined once the shared
# event masks it reads are in scope. The rank constants (``DOCK_PREVIOUS_RANK``,
# ``PERIOD_OWN_RANK``, ``DOCK_SAME_RANK``) and ``REBALANCE_RANK`` are derived
# from it there. An event source that runs as an explicit phase (the simulator)
# stamps its phase's rank straight onto the events it emits; a source with no
# phases (the historical loader) stamps it through :func:`stamp_history_ordering`,
# which applies :func:`phase_rank_by_timing`.

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
    """Mask of undocking events: a ``departed`` that takes a bike out of a dock."""
    return (flows["event_type"] == "departed") & (flows["move_id"] == 0)


def is_user_departure(flows: pd.DataFrame) -> pd.Series:
    """Mask of real user departures: an undocking ``departed`` of a user trip."""
    return is_undocking(flows) & (flows["flow_type"] == "user_trip")


def is_docking(flows: pd.DataFrame) -> pd.Series:
    """Mask of docking events: an ``arrived`` lands a bike at ``realized_target_id``."""
    return flows["event_type"].isin(DOCKING_EVENT_TYPES)


# ---------------------------------------------------------------------------
# The canonical phase order (one ordered declaration, Notations.md §0.1)
# ---------------------------------------------------------------------------
# The rule "dock-previous (0) < form-departures (1) < dock-same (2)" is stated
# once, here. Everything that needs the order reads it: the rank constants below,
# the historical loader's timing rule (:func:`phase_rank_by_timing`), and the
# simulator's phase list (``canonical_phases`` derives its order from this).
def _recognizes_period_own(flows: pd.DataFrame) -> pd.Series:
    """Mask of the period's own activity: a real user departure or a stockout loss."""
    is_stockout = (flows["event_type"] == "lost") & (flows["reason"] == "stockout")
    return is_user_departure(flows) | is_stockout


def _recognizes_dock_same(flows: pd.DataFrame) -> pd.Series:
    """Mask of docking-phase events for a flow that opened in this same period."""
    return ~_recognizes_period_own(flows) & (flows["period_id"] == flows["start_period"])


@dataclasses.dataclass(frozen=True)
class PhaseSpec:
    """One canonical intra-period phase, in rank order."""

    name: str
    rank: int
    recognizes: Callable[[pd.DataFrame], pd.Series] | None


#: The three user-trip phases, in the order they run inside a period. The rank
#: is the position, so the order is authored only here. The next phase after
#: them (rebalancing) takes ``len(CANONICAL_PHASE_ORDER)`` -- ``REBALANCE_RANK``.
CANONICAL_PHASE_ORDER: tuple[PhaseSpec, ...] = tuple(
    PhaseSpec(name=name, rank=rank, recognizes=recognizes)
    for rank, (name, recognizes) in enumerate(
        [
            ("dock_previous", None),
            ("period_own", _recognizes_period_own),
            ("dock_same", _recognizes_dock_same),
        ]
    )
)

_RANK_BY_NAME = {spec.name: spec.rank for spec in CANONICAL_PHASE_ORDER}
DOCK_PREVIOUS_RANK = _RANK_BY_NAME["dock_previous"]  # dock bikes that left in an earlier period
PERIOD_OWN_RANK = _RANK_BY_NAME["period_own"]  # this period's departures and stockout losses
DOCK_SAME_RANK = _RANK_BY_NAME["dock_same"]  # dock bikes that left and arrive within this period
REBALANCE_RANK = len(CANONICAL_PHASE_ORDER)  # the period's truck pickups and dropoffs (§14)


def _event_deltas(events: pd.DataFrame, extra_cols: tuple[str, ...] = ()) -> pd.DataFrame:
    """One signed ``delta`` per inventory-moving event, at the facility it touches."""
    cols = [*extra_cols, "facility_id", "commodity_category", "delta"]
    dock = events[is_docking(events)].copy()
    dock["facility_id"] = dock["realized_target_id"]
    dock["delta"] = dock["quantity"].astype("int64")
    undock = events[is_undocking(events)].copy()
    undock["facility_id"] = undock["source_id"]
    undock["delta"] = -undock["quantity"].astype("int64")
    return pd.concat([dock[cols], undock[cols]], ignore_index=True)


def inventory_deltas_from_events(events: pd.DataFrame) -> pd.DataFrame:
    """Net inventory change one batch of events implies, per (facility, commodity)."""
    return (
        _event_deltas(events)
        .groupby(["facility_id", "commodity_category"], as_index=False)["delta"]
        .sum()
    )


def occupancy_per_facility(
    frame: pd.DataFrame,
    value_column: str = "quantity",
    extra_keys: tuple[str, ...] = (),
) -> pd.Series:
    """Occupancy: bikes docked per facility, ``value_column`` summed across commodities."""
    return frame.groupby([*extra_keys, "facility_id"])[value_column].sum()


# ---------------------------------------------------------------------------
# Flow-event builders
# ---------------------------------------------------------------------------
def _typed_events(events_df: pd.DataFrame) -> pd.DataFrame:
    """Cast event columns to the canonical dtypes so frames concat cleanly."""
    for col, dtype in FLOW_EVENT_DTYPES.items():
        if col in events_df.columns:
            events_df[col] = events_df[col].astype(dtype)
    return events_df


def departed_events(trips: pd.DataFrame) -> pd.DataFrame:
    """One ``departed`` event row per trip leaving this period (move 0, event 0)."""
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
    """One ``arrived`` event row per in-transit flow docking at ``period_id``."""
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
    """One ``redirected`` *bounce* event per overflow flow (arc ``m``, event ``2m + 1``)."""
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
    """One ``departed`` row per redirected flow: the new leg after a bounce."""
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
    """One ``lost`` event per trip that did not happen, tagged with ``reason``."""
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
    """One ``departed`` row per bike a truck picks up (``flow_type="rebalance"``)."""
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
    """One ``arrived`` row per rebalance flow whose truck drops it off at ``period_id``."""
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
    """Return the in-transit set after one batch of events."""
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
    """Empty append-only flow journal."""
    return pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in FLOW_EVENT_DTYPES.items()})


def phase_rank_by_timing(flows: pd.DataFrame) -> pd.Series:
    """Derive each event's ``phase_rank`` from its own columns (the write-time rule)."""
    rank = pd.Series(CANONICAL_PHASE_ORDER[0].rank, index=flows.index, dtype="int64")
    for spec in CANONICAL_PHASE_ORDER:
        if spec.recognizes is not None:
            rank = rank.mask(spec.recognizes(flows), spec.rank)
    return rank


def stamp_history_ordering(journal: pd.DataFrame) -> pd.DataFrame:
    """Stamp the three ordering columns on a journal built without phases."""
    flows = journal.copy()
    flows["phase_rank"] = phase_rank_by_timing(flows)
    flows["phase_round"] = pd.Series(0, index=flows.index, dtype="int64")
    # ngroup with sort=True numbers the distinct labels in sorted (= step) order.
    keys = ["period_id", "phase_rank", "phase_round"]
    flows["step_id"] = flows.groupby(keys, sort=True).ngroup()
    return flows


def finalize_flows(journal: pd.DataFrame) -> pd.DataFrame:
    """Order the accumulated journal by its stamped steps and project the columns."""
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
    """Outflow per period and source: count of move-0 ``departed`` events."""
    departed = flows[is_user_departure(flows)]
    return (
        departed.groupby(["period_id", "source_id", "commodity_category"], as_index=False)[
            "quantity"
        ]
        .sum()
        .rename(columns={"source_id": "facility_id"})
    )


def flows_to_arrivals(flows: pd.DataFrame) -> pd.DataFrame:
    """Inflow per period and target: count of docking events."""
    docked = flows[is_docking(flows)]
    keys = ["period_id", "realized_target_id", "commodity_category"]
    return (
        docked.groupby(keys, as_index=False)["quantity"]
        .sum()
        .rename(columns={"realized_target_id": "facility_id"})
    )


def flows_to_redirects(flows: pd.DataFrame) -> pd.DataFrame:
    """Bounces per period and facility: ``redirected`` events at the full target."""
    bounced = flows[flows["event_type"] == "redirected"]
    return (
        bounced.groupby(["period_id", "planned_target_id", "commodity_category"], as_index=False)[
            "quantity"
        ]
        .sum()
        .rename(columns={"planned_target_id": "facility_id"})
    )


def flows_to_losses(flows: pd.DataFrame, reason: str) -> pd.DataFrame:
    """Losses per period and facility for one loss reason."""
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
    """Per-period inventory as a pure function of the journal and initial inventory."""
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
    """Build the per-(period, facility, commodity) state of the run, in one table."""
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
    """Per-event deltas with ``step_id`` and ``period_id`` kept for cumulation."""
    return _event_deltas(flows, ("step_id", "period_id"))


def inventory_at_moments(flows: pd.DataFrame, initial_inventory: pd.DataFrame) -> pd.DataFrame:
    """Inventory just before and just after every inventory step, for all facilities."""
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
    """Widen the journal with the inventory of each event's own facility."""
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
    """Widen the journal with each event's riding time so far and the money it accrued."""
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
    """Widen the journal with the measures: duration, distance and money columns."""
    out = flows.copy()
    out["planned_duration_periods"] = out["planned_end_period"] - out["start_period"]
    out["realized_duration_periods"] = out["realized_end_period"] - out["start_period"]
    out["planned_distance_km"] = routes.distance_km(out["source_id"], out["planned_target_id"])
    out["realized_distance_km"] = routes.distance_km(out["source_id"], out["realized_target_id"])
    return flows_with_costs(out, rates, period_len)


_EARTH_RADIUS_KM = 6371.0088


def haversine_km(lat1: pd.Series, lng1: pd.Series, lat2: pd.Series, lng2: pd.Series) -> pd.Series:
    """Great-circle distance in kilometres between two coordinate columns."""
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
    """Rank neighbours by the one metric: squared Euclidean distance on (lat, lng)."""
    return (lat - other_lat) ** 2 + (lng - other_lng) ** 2


def _squared_distances(geo: pd.DataFrame, facility_id: str) -> pd.Series:
    """Squared distance from ``facility_id`` to every other facility, nearest first."""
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
    """Explain one redirect: its full station's neighbours, in distance order, at the moment."""
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

    # Dock occupancy at the redirect step (occupancy_per_facility: shared docks,
    # so the total across commodities). inventory_at_moments lists every facility
    # at every step, so a neighbour with no bikes still appears; reindex fills
    # any that never held one with 0.
    moments = inventory_at_moments(flows, initial_inventory)
    at_step = moments[moments["step_id"] == step_id]
    occ = pd.DataFrame(
        {
            "inventory_before": occupancy_per_facility(at_step, "inventory_before"),
            "inventory_after": occupancy_per_facility(at_step, "inventory_after"),
        }
    ).reindex(order, fill_value=0)

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
    """I1 -- demand splits exactly into served departures and stockout losses."""
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
    """I2 -- every flow due by the horizon closes with exactly one terminal event."""
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
