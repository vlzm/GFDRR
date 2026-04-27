"""Built-in (universal) phases: DemandPhase and ArrivalsPhase.

These phases implement domain-agnostic logic that applies to any commodity
network.  Domain-specific phases (DispatchPhase, SupplyPhase, TransformPhase)
live in separate modules.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from gbp.consumers.simulator.phases import PhaseResult, Schedule
from gbp.consumers.simulator.state import INVENTORY_COLUMNS
from gbp.core.enums import ResourceStatus

if TYPE_CHECKING:
    from gbp.consumers.simulator.state import PeriodRow, SimulationState
    from gbp.core.model import ResolvedModelData


class DemandPhase:
    """Consume commodity from facility inventory based on resolved demand.

    For each demand row matching the current period, subtracts from inventory.
    Logs flow events (commodity leaving system) and any unmet demand (deficit).
    """

    name: str = "DEMAND"

    def __init__(self, schedule: Schedule | None = None) -> None:
        """Initialise with an optional schedule (default: every period)."""
        self._schedule = schedule or Schedule.every()

    def should_run(self, period: PeriodRow) -> bool:
        """Delegate to schedule."""
        return self._schedule.should_run(period)

    def execute(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
    ) -> PhaseResult:
        """Apply demand for the current period to inventory."""
        # 1. Filter demand for this period
        if resolved.demand is None:
            return PhaseResult.empty(state)

        period_demand = resolved.demand[
            resolved.demand["period_id"] == period.period_id
        ].copy()

        if period_demand.empty:
            return PhaseResult.empty(state)

        # Rename demand quantity to avoid collision with inventory quantity
        period_demand = period_demand.rename(columns={"quantity": "demand_qty"})

        # 2. Left-merge: keep all inventory, attach demand where it exists
        merged = state.inventory.merge(
            period_demand[["facility_id", "commodity_category", "demand_qty"]],
            on=["facility_id", "commodity_category"],
            how="left",
        )
        merged["demand_qty"] = merged["demand_qty"].fillna(0.0)

        # 3. Vectorized fulfilled / deficit
        merged["fulfilled"] = np.minimum(merged["quantity"], merged["demand_qty"])
        merged["deficit"] = merged["demand_qty"] - merged["fulfilled"]

        # 4. New inventory = old quantity - fulfilled
        new_inv = merged[["facility_id", "commodity_category"]].copy()
        new_inv["quantity"] = merged["quantity"] - merged["fulfilled"]

        # 5. Flow events (only rows where something was consumed)
        consumed = merged[merged["fulfilled"] > 0]
        flow_events = pd.DataFrame({
            "source_id": "EXT",
            "target_id": consumed["facility_id"].values,
            "commodity_category": consumed["commodity_category"].values,
            "modal_type": None,
            "quantity": consumed["fulfilled"].values,
            "resource_id": None,
        })

        # 6. Unmet demand (only rows with deficit > 0)
        unmet_rows = merged[merged["deficit"] > 0]
        unmet_demand = pd.DataFrame({
            "facility_id": unmet_rows["facility_id"].values,
            "commodity_category": unmet_rows["commodity_category"].values,
            "requested": unmet_rows["demand_qty"].values,
            "fulfilled": unmet_rows["fulfilled"].values,
            "deficit": unmet_rows["deficit"].values,
        })

        # 7. Return result
        return PhaseResult(
            state=state.with_inventory(new_inv),
            flow_events=flow_events,
            unmet_demand=unmet_demand,
            rejected_dispatches=pd.DataFrame(),
        )


_FLOW_EVENT_COLUMNS: tuple[str, ...] = (
    "source_id",
    "target_id",
    "commodity_category",
    "modal_type",
    "quantity",
    "resource_id",
)


def _period_flows(
    resolved: ResolvedModelData,
    period: PeriodRow,
) -> pd.DataFrame | None:
    """Return observed_flow rows for *period*, or ``None`` if empty."""
    if resolved.observed_flow is None or resolved.observed_flow.empty:
        return None
    flows = resolved.observed_flow[
        resolved.observed_flow["period_id"] == period.period_id
    ]
    if flows.empty:
        return None
    return flows


class OrganicDeparturePhase:
    """Subtract outflow from source facility inventory.

    For each ``observed_flow`` row matching the current period, subtracts the
    trip quantity from the source facility's inventory.  Emits flow events
    (one per observed row, with both ``source_id`` and ``target_id``).

    Inventory is **not** clipped at zero — a facility may go transiently
    negative within a period.  The corresponding :class:`OrganicArrivalPhase`
    adds inflow and clips the result, restoring exact parity with the
    original :class:`OrganicFlowPhase`.
    """

    name: str = "ORGANIC_DEPARTURE"

    def __init__(self, schedule: Schedule | None = None) -> None:
        """Initialise with an optional schedule (default: every period)."""
        self._schedule = schedule or Schedule.every()

    def should_run(self, period: PeriodRow) -> bool:
        """Delegate to schedule."""
        return self._schedule.should_run(period)

    def execute(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
    ) -> PhaseResult:
        """Subtract outflow and emit flow events for the current period."""
        flows = _period_flows(resolved, period)
        if flows is None:
            return PhaseResult.empty(state)

        outflow = (
            flows.groupby(["source_id", "commodity_category"], as_index=False)["quantity"]
            .sum()
            .rename(columns={"source_id": "facility_id", "quantity": "outflow"})
        )

        new_inv = state.inventory.merge(
            outflow, on=["facility_id", "commodity_category"], how="left",
        )
        new_inv["outflow"] = new_inv["outflow"].fillna(0.0)
        new_inv["quantity"] = new_inv["quantity"] - new_inv["outflow"]
        new_inv = new_inv[INVENTORY_COLUMNS].copy()

        flow_events = pd.DataFrame({
            col: (
                flows[col].values if col in flows.columns
                else [None] * len(flows)
            )
            for col in _FLOW_EVENT_COLUMNS
        })

        return PhaseResult(
            state=state.with_inventory(new_inv),
            flow_events=flow_events,
            unmet_demand=pd.DataFrame(),
            rejected_dispatches=pd.DataFrame(),
        )


class OrganicArrivalPhase:
    """Add inflow to target facility inventory and clip at zero.

    For each ``observed_flow`` row matching the current period, adds the
    trip quantity to the target facility's inventory and clips the result
    at zero.  No flow events are emitted — they are already produced by
    :class:`OrganicDeparturePhase`.

    Together, ``[OrganicDeparturePhase, OrganicArrivalPhase]`` produce the
    same final inventory as the original :class:`OrganicFlowPhase`.
    """

    name: str = "ORGANIC_ARRIVAL"

    def __init__(self, schedule: Schedule | None = None) -> None:
        """Initialise with an optional schedule (default: every period)."""
        self._schedule = schedule or Schedule.every()

    def should_run(self, period: PeriodRow) -> bool:
        """Delegate to schedule."""
        return self._schedule.should_run(period)

    def execute(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
    ) -> PhaseResult:
        """Add inflow and clip inventory for the current period."""
        flows = _period_flows(resolved, period)
        if flows is None:
            return PhaseResult.empty(state)

        inflow = (
            flows.groupby(["target_id", "commodity_category"], as_index=False)["quantity"]
            .sum()
            .rename(columns={"target_id": "facility_id", "quantity": "inflow"})
        )

        new_inv = state.inventory.merge(
            inflow, on=["facility_id", "commodity_category"], how="left",
        )
        new_inv["inflow"] = new_inv["inflow"].fillna(0.0)
        new_inv["quantity"] = (new_inv["quantity"] + new_inv["inflow"]).clip(lower=0.0)
        new_inv = new_inv[INVENTORY_COLUMNS].copy()

        return PhaseResult(
            state=state.with_inventory(new_inv),
            flow_events=pd.DataFrame(),
            unmet_demand=pd.DataFrame(),
            rejected_dispatches=pd.DataFrame(),
        )


class OrganicFlowPhase:
    """Replay ``resolved.observed_flow`` as flow events, instant transfer.

    Convenience wrapper that runs :class:`OrganicDeparturePhase` followed by
    :class:`OrganicArrivalPhase` in a single phase call.  Produces identical
    results to using the two phases separately.

    Use this in place of :class:`DemandPhase` when the target side of every
    organic movement is known (e.g. bike-share trips where each demand event
    has a known destination station).
    """

    name: str = "ORGANIC_FLOW"

    def __init__(self, schedule: Schedule | None = None) -> None:
        """Initialise with an optional schedule (default: every period)."""
        self._schedule = schedule or Schedule.every()
        self._departure = OrganicDeparturePhase(self._schedule)
        self._arrival = OrganicArrivalPhase(self._schedule)

    def should_run(self, period: PeriodRow) -> bool:
        """Delegate to schedule."""
        return self._schedule.should_run(period)

    def execute(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
    ) -> PhaseResult:
        """Replay observed_flow for the current period."""
        dep_result = self._departure.execute(state, resolved, period)
        arr_result = self._arrival.execute(dep_result.state, resolved, period)

        return PhaseResult(
            state=arr_result.state,
            flow_events=dep_result.flow_events,
            unmet_demand=pd.DataFrame(),
            rejected_dispatches=pd.DataFrame(),
        )


class ArrivalsPhase:
    """Process shipments arriving at their destination in the current period.

    Filters ``state.in_transit`` for shipments whose ``arrival_period`` matches
    the current ``period.period_index``, transfers commodity into target facility
    inventory, and updates resource statuses from IN_TRANSIT to AVAILABLE.
    """

    name: str = "ARRIVALS"

    def __init__(self, schedule: Schedule | None = None) -> None:
        """Initialise with an optional schedule (default: every period)."""
        self._schedule = schedule or Schedule.every()

    def should_run(self, period: PeriodRow) -> bool:
        """Delegate to schedule."""
        return self._schedule.should_run(period)

    def execute(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
    ) -> PhaseResult:
        """Process arrivals for the current period."""
        if state.in_transit.empty:
            return PhaseResult.empty(state)

        # 1. Split in_transit into arriving vs remaining
        mask_arriving = state.in_transit["arrival_period"] == period.period_index
        arriving = state.in_transit[mask_arriving]
        remaining = state.in_transit[~mask_arriving].copy()

        if arriving.empty:
            return PhaseResult.empty(state)

        # 2. Group arriving quantities by (target_id, commodity_category)
        arrival_totals = (
            arriving
            .groupby(["target_id", "commodity_category"], as_index=False)["quantity"]
            .sum()
            .rename(columns={"target_id": "facility_id", "quantity": "arrived_qty"})
        )

        # 3. Merge into inventory and add arrived quantities
        new_inv = state.inventory.merge(
            arrival_totals,
            on=["facility_id", "commodity_category"],
            how="left",
        )
        new_inv["arrived_qty"] = new_inv["arrived_qty"].fillna(0.0)
        new_inv["quantity"] = new_inv["quantity"] + new_inv["arrived_qty"]
        new_inv = new_inv[INVENTORY_COLUMNS].copy()

        # 4. Update resources for arriving shipments
        resources = state.resources.copy()
        arriving_resources = arriving[
            arriving["resource_id"].notna()
        ][["resource_id", "target_id"]].drop_duplicates(subset=["resource_id"])

        if not arriving_resources.empty:
            res_map = arriving_resources.set_index("resource_id")["target_id"]
            mask = resources["resource_id"].isin(res_map.index)
            resources.loc[mask, "status"] = ResourceStatus.AVAILABLE.value
            resources.loc[mask, "available_at_period"] = None
            resources.loc[mask, "current_facility_id"] = (
                resources.loc[mask, "resource_id"].map(res_map)
            )

        # 5. Build flow events (one row per arriving shipment)
        flow_events = pd.DataFrame({
            "source_id": arriving["source_id"].values,
            "target_id": arriving["target_id"].values,
            "commodity_category": arriving["commodity_category"].values,
            "modal_type": arriving.get("modal_type", pd.Series(dtype=str)).values
                if "modal_type" in arriving.columns
                else [None] * len(arriving),
            "quantity": arriving["quantity"].values,
            "resource_id": arriving["resource_id"].values,
        })

        # 6. Return updated state
        new_state = (
            state
            .with_inventory(new_inv)
            .with_in_transit(remaining)
            .with_resources(resources)
        )
        return PhaseResult(
            state=new_state,
            flow_events=flow_events,
            unmet_demand=pd.DataFrame(),
            rejected_dispatches=pd.DataFrame(),
        )
