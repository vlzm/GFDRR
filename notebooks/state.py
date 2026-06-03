import dataclasses
from typing import Any


# ===========================================================================
# Simulator engine -- historical replay (minimal working version).
#
# The base scenario re-emits every historical trip exactly, so that
#     simulated_flows_df == historical_flows_df.
# Inventory and in-transit are tracked as real state, but the constraints that
# *change* outcomes (dock overflow -> redirect, stockout -> lost) live in the
# phases as TODO skeletons: they cannot fire in an exact replay and only become
# meaningful once demand is pushed above the historical baseline.
# ===========================================================================


# -- Flow-event builders ----------------------------------------------------
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


# -- Inventory updates ------------------------------------------------------
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


# -- Model data + run config ------------------------------------------------
class SimulatorConfigError(Exception):
    pass


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

# -- State ------------------------------------------------------------------
@dataclasses.dataclass(frozen=True)
class SimulationState:
    """Simulation facts at the current period (replaced, never mutated)."""

    period_id: int
    inventory: pd.DataFrame    # facility_id, commodity_category, quantity
    in_transit: pd.DataFrame   # departed flows not yet docked
    resources: pd.DataFrame    # trucks (unused until rebalancing); empty for now
    intermediates: dict[str, Any] = dataclasses.field(default_factory=dict)

    def with_inventory(self, new_inventory: pd.DataFrame) -> "SimulationState":
        return dataclasses.replace(self, inventory=new_inventory)

    def with_in_transit(self, new_in_transit: pd.DataFrame) -> "SimulationState":
        return dataclasses.replace(self, in_transit=new_in_transit)

    def with_resources(self, new_resources: pd.DataFrame) -> "SimulationState":
        return dataclasses.replace(self, resources=new_resources)

    def with_intermediates(self, **updates: Any) -> "SimulationState":
        return dataclasses.replace(self, intermediates={**self.intermediates, **updates})

    def advance_period(self, next_period_id: int) -> "SimulationState":
        # Intermediates are transient per-period hand-offs between phases.
        return dataclasses.replace(self, period_id=next_period_id, intermediates={})


@dataclasses.dataclass
class PhaseResult:
    """What a phase returns: the next state plus any flow events it emitted."""

    state: SimulationState
    events: Any = None   # DataFrame of new flow-event rows, or None

    @classmethod
    def empty(cls, state: SimulationState) -> "PhaseResult":
        return cls(state=state, events=None)


# -- Log --------------------------------------------------------------------
class SimulationLog:
    """Collects flow events and end-of-period inventory snapshots."""

    def __init__(self) -> None:
        self.events: list = []
        self.inventory_snapshots: list = []

    def record_events(self, result: PhaseResult, phase_name: str, period: PeriodRow) -> None:
        if result.events is not None and len(result.events):
            self.events.append(result.events)

    def record_period(self, state: SimulationState, period: PeriodRow) -> None:
        self.inventory_snapshots.append(state.inventory.assign(period_id=period.period_id))

    def to_flows_df(self) -> pd.DataFrame:
        """Assemble the event log exactly like ``get_historical_flows_df``."""
        events = pd.concat(self.events, ignore_index=True)
        for col, dtype in FLOW_EVENT_DTYPES.items():
            events[col] = events[col].astype(dtype)
        events = events.sort_values(["period_id", "flow_id", "event_order"]).reset_index(drop=True)
        events["event_id"] = events.index.astype("Int64")
        return events[FLOW_EVENT_COLUMNS]

    def to_inventory_df(self) -> pd.DataFrame:
        inv = pd.concat(self.inventory_snapshots, ignore_index=True)
        return inv[["period_id", "facility_id", "commodity_category", "quantity"]]