"""Simulator state primitives: flow-event schema, event builders, inventory
updates, the per-period state object, and the observation derivations.

This module is the low-level foundation of the simulator. It has no dependency
on the data loaders or the engine, so both can import from it freely.

The flow journal is the single source of truth for what happened in a run.
Inventory and the in-transit working set are *projections* of that journal:
they are maintained incrementally for speed (re-deriving them from the full
journal every period would be O(N) per period), but they carry no information
the journal does not. The marginal observations (departures, arrivals, demand,
supply, OD matrix) are pure functions of the journal and the current inventory,
so historical and simulated runs share one definition for each of them.
"""

import dataclasses
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Flow-event schema
# ---------------------------------------------------------------------------
FLOW_EVENT_COLUMNS = [
    "event_id", "period_id", "flow_id", "flow_type", "event_type", "commodity_category",
    "source_id", "planned_target_id", "realized_target_id",
    "start_period", "planned_end_period", "realized_end_period",
    "resource_id", "quantity", "reason",
]

FLOW_EVENT_DTYPES = {
    "period_id": "Int64",
    "flow_id": "string",
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


# ---------------------------------------------------------------------------
# Flow-event builders
# ---------------------------------------------------------------------------
def _typed_events(events_df: pd.DataFrame) -> pd.DataFrame:
    """Cast event columns to the canonical dtypes so frames concat cleanly."""
    for col, dtype in FLOW_EVENT_DTYPES.items():
        events_df[col] = events_df[col].astype(dtype)
    events_df["event_order"] = events_df["event_order"].astype("int64")
    return events_df


def departed_events(trips: pd.DataFrame) -> pd.DataFrame:
    """One ``departed`` event row per trip leaving this period."""
    return _typed_events(pd.DataFrame({
        "period_id":           trips["start_period"],
        "flow_id":             trips["flow_id"],
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
        "event_order":         0,
    }))


def arrived_events(in_transit_due: pd.DataFrame, period_id: int) -> pd.DataFrame:
    """One ``arrived`` event row per in-transit flow docking this period."""
    return _typed_events(pd.DataFrame({
        "period_id":           period_id,
        "flow_id":             in_transit_due["flow_id"],
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
        "event_order":         1,
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

    Holds the same columns the event builders emit (the canonical flow fields
    plus the transient ``event_order``); ``event_id`` is assigned only once at
    :func:`finalize_flows`, so it is absent while the journal is still growing.
    """
    journal = pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in FLOW_EVENT_DTYPES.items()})
    journal["event_order"] = pd.Series(dtype="int64")
    return journal


def finalize_flows(journal: pd.DataFrame) -> pd.DataFrame:
    """Order the accumulated journal and assign a monotonic ``event_id``.

    Mirrors :func:`dataloader_graph.get_historical_flows_df` exactly (same sort
    keys, same ``event_id`` assignment, same column projection), so a replay
    run's finalized journal is identical to the historical log.

    Parameters
    ----------
    journal : pandas.DataFrame
        The append-only journal accumulated during a run.

    Returns
    -------
    pandas.DataFrame
        Event log with columns :data:`FLOW_EVENT_COLUMNS`, sorted by
        ``period_id`` then ``flow_id`` then ``event_order``, with a monotonic
        ``event_id``.
    """
    flows = journal.copy()
    for col, dtype in FLOW_EVENT_DTYPES.items():
        flows[col] = flows[col].astype(dtype)
    flows = flows.sort_values(["period_id", "flow_id", "event_order"]).reset_index(drop=True)
    flows["event_id"] = flows.index.astype("Int64")
    return flows[FLOW_EVENT_COLUMNS]


# ---------------------------------------------------------------------------
# Inventory updates
# ---------------------------------------------------------------------------
def adjust_inventory(inventory: pd.DataFrame, deltas: pd.DataFrame) -> pd.DataFrame:
    """Add signed ``delta`` per (facility_id, commodity_category)."""
    out = inventory.merge(deltas, on=["facility_id", "commodity_category"], how="outer")
    out["quantity"] = out["quantity"].fillna(0) + out["delta"].fillna(0)
    return out[["facility_id", "commodity_category", "quantity"]]


def departure_deltas(trips: pd.DataFrame) -> pd.DataFrame:
    """-1 per departing bike, grouped by (source, commodity)."""
    deltas = (
        trips.groupby(["source_id", "commodity_category"]).size()
        .reset_index(name="delta").rename(columns={"source_id": "facility_id"})
    )
    deltas["delta"] = -deltas["delta"]
    return deltas


def arrival_deltas(in_transit_due: pd.DataFrame) -> pd.DataFrame:
    """+1 per docking bike, grouped by (target, commodity)."""
    return (
        in_transit_due.groupby(["planned_target_id", "commodity_category"]).size()
        .reset_index(name="delta").rename(columns={"planned_target_id": "facility_id"})
    )


# ---------------------------------------------------------------------------
# Observation derivations (pure functions of the flow journal)
# ---------------------------------------------------------------------------
def flows_to_departures(flows: pd.DataFrame) -> pd.DataFrame:
    """Outflow per period and origin: count of ``departed`` events.

    Grouped by ``(period_id, facility_id, commodity_category)`` where
    ``facility_id`` is the trip's ``source_id``.
    """
    return (
        flows[flows["event_type"] == "departed"]
        .groupby(["period_id", "source_id", "commodity_category"], as_index=False)["quantity"].sum()
        .rename(columns={"source_id": "facility_id"})
    )


def flows_to_arrivals(flows: pd.DataFrame) -> pd.DataFrame:
    """Inflow per period and destination: count of ``arrived`` events.

    Grouped by ``(period_id, facility_id, commodity_category)`` where
    ``facility_id`` is the trip's ``realized_target_id``.
    """
    return (
        flows[flows["event_type"] == "arrived"]
        .groupby(["period_id", "realized_target_id", "commodity_category"], as_index=False)["quantity"].sum()
        .rename(columns={"realized_target_id": "facility_id"})
    )


def flows_to_od_matrix(flows: pd.DataFrame) -> pd.DataFrame:
    """Origin-destination demand matrix from ``departed`` events.

    Counts trips per ``(source_id, planned_target_id, commodity_category)``
    across the whole journal (the intended origin->destination structure of
    demand, not the realized docking which may be redirected above baseline).
    """
    return (
        flows[flows["event_type"] == "departed"]
        .groupby(["source_id", "planned_target_id", "commodity_category"], as_index=False)["quantity"].sum()
        .rename(columns={"quantity": "count"})
    )


def get_inventory_df(flows: pd.DataFrame, initial_inventory: pd.DataFrame) -> pd.DataFrame:
    """Per-period inventory as a pure function of the journal and initial stock.

    Inventory at the end of period ``t`` equals the initial stock plus the
    cumulative net flow (arrivals ``+1``, departures ``-1``) up to and including
    ``t``, per ``(facility_id, commodity_category)``. Because both historical and
    simulated inventory are defined this way, they need no per-period snapshot
    table — the journal is enough.

    Parameters
    ----------
    flows : pandas.DataFrame
        A flow-event log (historical or finalized simulated).
    initial_inventory : pandas.DataFrame
        Starting stock with ``facility_id``, ``commodity_category``, ``quantity``.

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
        flows[flows["event_type"] == "departed"]
        .groupby(["period_id", "source_id", "commodity_category"], as_index=False)["quantity"].sum()
        .rename(columns={"source_id": "facility_id", "quantity": "delta"})
    )
    dep["delta"] = -dep["delta"]
    arr = (
        flows[flows["event_type"] == "arrived"]
        .groupby(["period_id", "realized_target_id", "commodity_category"], as_index=False)["quantity"].sum()
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


# ---------------------------------------------------------------------------
# Run config + scheduling
# ---------------------------------------------------------------------------
class SimulatorConfigError(Exception):
    """Raised when an environment is built with insufficient inputs."""


@dataclasses.dataclass(frozen=True)
class Schedule:
    """When a phase runs: every ``every_n`` periods (1 = every period)."""

    every_n: int = 1

    @classmethod
    def every(cls) -> "Schedule":
        return cls(1)

    @classmethod
    def every_n_periods(cls, n: int) -> "Schedule":
        return cls(n)

    def should_run(self, period: "PeriodRow") -> bool:
        return period.period_id % self.every_n == 0


@dataclasses.dataclass(frozen=True)
class PeriodRow:
    """One row of the period grid (the simulation clock)."""

    period_id: int
    start_timestamp: Any
    end_timestamp: Any


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
@dataclasses.dataclass(frozen=True)
class SimulationState:
    """Simulation facts at the current period (replaced, never mutated).

    ``state_flows_df`` is the append-only event journal and the single source of
    truth for what happened. ``state_inventory_df`` and ``in_transit`` are
    materialized projections of that journal, kept incrementally for speed.
    ``in_transit`` is deliberately *not* part of the named state contract: it is
    internal plumbing (the "departed but not yet docked" working set), the same
    kind of projection as inventory but not worth observing on its own.

    The "Additional" observations are exposed as read-only properties derived on
    demand: ``state_departures_df``, ``state_arrivals_df``, ``state_demand_df``,
    ``state_supply_df`` and ``state_od_matrix_df``.

    Attributes
    ----------
    state_period_id_obj : PeriodRow
        The current period (the clock position).
    state_inventory_df : pandas.DataFrame
        Current stock: ``facility_id``, ``commodity_category``, ``quantity``.
    state_flows_df : pandas.DataFrame
        Append-only flow-event journal accumulated so far this run.
    state_resources_df : pandas.DataFrame
        Resource (truck) observations; empty in the historical replay.
    in_transit : pandas.DataFrame
        Internal projection: ``departed`` flows not yet docked.
    intermediates : dict
        Transient per-period hand-offs between phases.
    """

    state_period_id_obj: PeriodRow
    state_inventory_df: pd.DataFrame
    state_flows_df: pd.DataFrame
    state_resources_df: pd.DataFrame
    in_transit: pd.DataFrame = dataclasses.field(default_factory=empty_in_transit)
    intermediates: dict[str, Any] = dataclasses.field(default_factory=dict)

    # -- clock ---------------------------------------------------------------
    @property
    def period_id(self) -> int:
        """Integer id of the current period."""
        return self.state_period_id_obj.period_id

    # -- derived "Additional" observations -----------------------------------
    @property
    def state_departures_df(self) -> pd.DataFrame:
        """Outflow per period and origin, derived from ``state_flows_df``."""
        return flows_to_departures(self.state_flows_df)

    @property
    def state_arrivals_df(self) -> pd.DataFrame:
        """Inflow per period and destination, derived from ``state_flows_df``."""
        return flows_to_arrivals(self.state_flows_df)

    @property
    def state_demand_df(self) -> pd.DataFrame:
        """Realized user demand (= departures in an exact replay).

        Demand is the number of trips users *wanted* to take. Above the
        historical baseline some of it is lost to stockouts and diverges from
        ``state_departures_df``; in the replay every desired trip departs, so the
        two coincide and demand is read off the journal as the departures.
        """
        return flows_to_departures(self.state_flows_df)

    @property
    def state_supply_df(self) -> pd.DataFrame:
        """Bikes currently available to depart: the current inventory."""
        return self.state_inventory_df

    @property
    def state_od_matrix_df(self) -> pd.DataFrame:
        """Origin-destination demand matrix, derived from ``state_flows_df``."""
        return flows_to_od_matrix(self.state_flows_df)

    # -- functional updates --------------------------------------------------
    def with_inventory(self, new_inventory: pd.DataFrame) -> "SimulationState":
        return dataclasses.replace(self, state_inventory_df=new_inventory)

    def with_in_transit(self, new_in_transit: pd.DataFrame) -> "SimulationState":
        return dataclasses.replace(self, in_transit=new_in_transit)

    def with_resources(self, new_resources: pd.DataFrame) -> "SimulationState":
        return dataclasses.replace(self, state_resources_df=new_resources)

    def append_flows(self, events: pd.DataFrame | None) -> "SimulationState":
        """Append a phase's emitted events to the journal (source of truth)."""
        if events is None or not len(events):
            return self
        journal = pd.concat([self.state_flows_df, events], ignore_index=True)
        return dataclasses.replace(self, state_flows_df=journal)

    def with_intermediates(self, **updates: Any) -> "SimulationState":
        return dataclasses.replace(self, intermediates={**self.intermediates, **updates})

    def advance_period(self, next_period_obj: PeriodRow) -> "SimulationState":
        # Intermediates are transient per-period hand-offs between phases.
        return dataclasses.replace(self, state_period_id_obj=next_period_obj, intermediates={})


@dataclasses.dataclass
class PhaseResult:
    """What a phase returns: the next state plus any flow events it emitted."""

    state: SimulationState
    events: Any = None   # DataFrame of new flow-event rows, or None

    @classmethod
    def empty(cls, state: SimulationState) -> "PhaseResult":
        return cls(state=state, events=None)
