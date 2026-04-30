"""Built-in (universal) phases: DemandPhase and ArrivalsPhase.

These phases implement domain-agnostic logic that applies to any commodity
network.  Domain-specific phases (DispatchPhase, SupplyPhase, TransformPhase)
live in separate modules.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import numpy as np
import pandas as pd

from gbp.consumers.simulator.inventory import (
    apply_delta,
    merge_with_inventory,
    to_inventory_delta,
)
from gbp.consumers.simulator.phases import PhaseResult, Schedule
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
        period_demand = resolved.demand[
            resolved.demand["period_id"] == period.period_id
        ].copy()

        if period_demand.empty:
            return PhaseResult.empty(state)

        # 2. Left-merge demand onto inventory; demand_qty = 0 where absent.
        period_demand = period_demand.rename(columns={"quantity": "demand_qty"})
        merged = merge_with_inventory(
            state.inventory,
            period_demand[["facility_id", "commodity_category", "demand_qty"]],
            value_col="demand_qty",
        )

        # 3. Vectorized fulfilled / deficit (consumption clipped at stock).
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
        events: dict[str, pd.DataFrame] = {}
        if not flow_events.empty:
            events["flow_events"] = flow_events
        if not unmet_demand.empty:
            events["unmet_demand"] = unmet_demand
        return PhaseResult(state=state.with_inventory(new_inv), events=events)


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
    if resolved.observed_flow.empty:
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

        outflow = to_inventory_delta(flows, facility_col="source_id")
        new_inv = apply_delta(state.inventory, outflow, op="subtract")

        flow_events = pd.DataFrame({
            col: (
                flows[col].values if col in flows.columns
                else [None] * len(flows)
            )
            for col in _FLOW_EVENT_COLUMNS
        })

        return PhaseResult(
            state=state.with_inventory(new_inv),
            events={"flow_events": flow_events} if not flow_events.empty else {},
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

        inflow = to_inventory_delta(flows, facility_col="target_id")
        new_inv = apply_delta(state.inventory, inflow, op="add_clip_zero")

        return PhaseResult(state=state.with_inventory(new_inv))


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
            events={**dep_result.events, **arr_result.events},
        )


class HistoricalLatentDemandPhase:
    """Expose historical departures/arrivals as latent marginals for the period.

    Reads ``resolved.observed_flow`` for the current period and computes per
    facility:

    - ``latent_departures`` (``O_i``): total outflow grouped by ``source_id``.
    - ``latent_arrivals``   (``D_j``): total inflow  grouped by ``target_id``.

    The result is written into ``state.intermediates["latent_demand"]`` for
    downstream phases (e.g. ``ODStructurePhase``, ``DeparturePhysicsPhase``)
    and emitted into ``simulation_latent_demand_log`` for inspection.

    This phase makes no inventory or in-transit changes — it only publishes
    "what people wanted" before any physics is applied.  Because the marginals
    are read directly from ``observed_flow``, downstream physics phases
    operating on the same data reproduce the historical record exactly.
    """

    name: str = "HISTORICAL_LATENT_DEMAND"

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
        """Compute O_i and D_j marginals from observed_flow for this period."""
        flows = _period_flows(resolved, period)
        if flows is None:
            return PhaseResult.empty(state)

        departures = (
            flows.groupby(["source_id", "commodity_category"], as_index=False)["quantity"]
            .sum()
            .rename(columns={
                "source_id": "facility_id",
                "quantity": "latent_departures",
            })
        )
        arrivals = (
            flows.groupby(["target_id", "commodity_category"], as_index=False)["quantity"]
            .sum()
            .rename(columns={
                "target_id": "facility_id",
                "quantity": "latent_arrivals",
            })
        )

        latent = departures.merge(
            arrivals, on=["facility_id", "commodity_category"], how="outer",
        )
        latent["latent_departures"] = latent["latent_departures"].fillna(0.0)
        latent["latent_arrivals"] = latent["latent_arrivals"].fillna(0.0)
        latent = latent.sort_values(
            ["facility_id", "commodity_category"], kind="stable",
        ).reset_index(drop=True)

        return PhaseResult(
            state=state.with_intermediates(latent_demand=latent),
            events={"latent_demand": latent.copy()},
        )


class HistoricalODStructurePhase:
    """Compute conditional destination probabilities ``P(j | i)`` from history.

    Reads ``resolved.observed_flow`` for the current period and builds, per
    ``(source_id, commodity_category)``, the conditional distribution over
    ``target_id``::

        P(target_id = j | source_id = i, commodity = k)
            = T_ijk / sum_j T_ijk

    where ``T_ijk`` is the observed quantity flowing from *i* to *j* in
    commodity *k* during the current period.

    The result is written into ``state.intermediates["od_probabilities"]``
    for downstream phases (e.g. trip sampling).  No log table is emitted:
    the OD matrix is potentially large (O(N^2) per period) and is meant
    as a transient hand-off between phases, not historical record.

    Inventory and in-transit are untouched.  By construction the probability
    column sums to 1.0 within every ``(source_id, commodity_category)`` group
    (modulo floating-point error), since the marginal ``O_i`` is recomputed
    from the same ``T_ij`` rather than read from elsewhere — guaranteeing
    self-consistency and parity with the data the next phase will sample.
    """

    name: str = "HISTORICAL_OD_STRUCTURE"

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
        """Build the conditional OD distribution for the current period."""
        flows = _period_flows(resolved, period)
        if flows is None:
            return PhaseResult.empty(state)

        joint = (
            flows.groupby(
                ["source_id", "target_id", "commodity_category"], as_index=False,
            )["quantity"]
            .sum()
            .rename(columns={"quantity": "joint"})
        )
        origin_total = (
            joint.groupby(["source_id", "commodity_category"], as_index=False)["joint"]
            .sum()
            .rename(columns={"joint": "origin_total"})
        )

        od = joint.merge(
            origin_total, on=["source_id", "commodity_category"], how="left",
        )
        od["probability"] = od["joint"] / od["origin_total"]
        od = od[["source_id", "target_id", "commodity_category", "probability"]]
        od = od.sort_values(
            ["source_id", "commodity_category", "target_id"], kind="stable",
        ).reset_index(drop=True)

        return PhaseResult(
            state=state.with_intermediates(od_probabilities=od),
        )


class DeparturePhysicsPhase:
    """Apply the inventory constraint to latent departures.

    Reads ``state.intermediates["latent_demand"]`` (published by an upstream
    phase such as :class:`HistoricalLatentDemandPhase`) and reduces facility
    inventory by the realised number of departures.

    Two modes:

    - ``permissive`` (default): ``realized = latent_departures``.  No clipping
      is applied, so inventory may go transiently negative — matching the
      behaviour of :class:`OrganicDeparturePhase`.  Use for historical
      replay, where ``O_i`` comes from observed data and is by construction
      feasible; clipping would break parity with the source.
    - ``strict``: ``realized = min(max(0, inventory), latent_departures)``.
      The gap ``latent - realized`` is recorded as lost demand.  Use for
      predictive scenarios where ``O_i`` is hypothetical and the simulator
      must enforce non-negative stock.

    The phase publishes ``intermediates["realized_departures"]`` for
    downstream trip sampling and emits ``simulation_lost_demand_log`` rows
    where ``lost > 0``.
    """

    name: str = "DEPARTURE_PHYSICS"

    def __init__(
        self,
        mode: Literal["permissive", "strict"] = "permissive",
        schedule: Schedule | None = None,
    ) -> None:
        """Initialise the phase.

        Args:
            mode: ``"permissive"`` for historical replay (no clipping),
                ``"strict"`` for predictive scenarios (enforce inventory >= 0).
            schedule: Optional schedule (default: every period).

        Raises:
            ValueError: If *mode* is not one of ``"permissive"`` or ``"strict"``.
        """
        if mode not in ("permissive", "strict"):
            msg = f"mode must be 'permissive' or 'strict', got {mode!r}"
            raise ValueError(msg)
        self._mode = mode
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
        """Apply the inventory constraint to the upstream latent demand."""
        latent = state.intermediates.get("latent_demand")
        if latent is None or latent.empty:
            return PhaseResult.empty(state)

        dep = latent.loc[
            latent["latent_departures"] > 0,
            ["facility_id", "commodity_category", "latent_departures"],
        ]
        if dep.empty:
            return PhaseResult.empty(state)

        merged = merge_with_inventory(
            state.inventory, dep, value_col="latent_departures",
        )

        if self._mode == "permissive":
            merged["realized"] = merged["latent_departures"]
        else:  # strict
            available = merged["quantity"].clip(lower=0.0)
            merged["realized"] = np.minimum(available, merged["latent_departures"])

        merged["lost"] = merged["latent_departures"] - merged["realized"]

        new_inv = merged[["facility_id", "commodity_category"]].copy()
        new_inv["quantity"] = merged["quantity"] - merged["realized"]

        realized_dep = (
            merged.loc[
                merged["realized"] > 0,
                ["facility_id", "commodity_category", "realized"],
            ]
            .rename(columns={"realized": "realized_departures"})
            .reset_index(drop=True)
        )

        lost_rows = merged[merged["lost"] > 0]
        lost_log = pd.DataFrame({
            "facility_id": lost_rows["facility_id"].values,
            "commodity_category": lost_rows["commodity_category"].values,
            "latent": lost_rows["latent_departures"].values,
            "realized": lost_rows["realized"].values,
            "lost": lost_rows["lost"].values,
        })

        new_state = (
            state
            .with_inventory(new_inv)
            .with_intermediates(realized_departures=realized_dep)
        )
        events = {"lost_demand": lost_log} if not lost_log.empty else {}
        return PhaseResult(state=new_state, events=events)


class HistoricalTripSamplingPhase:
    """Replay observed trips into ``state.in_transit`` for historical scenarios.

    For each row of ``resolved.observed_flow`` matching the current period,
    emits one shipment into ``state.in_transit`` with::

        shipment_id   = f"organic_trip_{period_index}_{i}"
        source_id     = observed_flow.source_id
        target_id     = observed_flow.target_id
        quantity      = observed_flow.quantity
        resource_id   = None
        departure_period = period_index
        arrival_period   = period_index   # instant, same-period arrival

    The same-period arrival reproduces the behaviour of
    :class:`OrganicArrivalPhase`, which adds inflow to the target in the
    same period as the source departure.  When a separate travel-time phase
    is introduced, this assumption will be relaxed and ``arrival_period``
    will become ``period_index + lead_time``.

    The phase does not modify inventory (that is the job of
    :class:`DeparturePhysicsPhase` upstream and :class:`ArrivalsPhase`
    downstream) and does not emit flow events (``ArrivalsPhase`` emits them
    on delivery).  Its sole effect is to populate ``in_transit``.

    Pipeline contract: must be placed after a permissive
    :class:`DeparturePhysicsPhase` and before :class:`ArrivalsPhase`.
    Pairing with strict-mode physics will desynchronise the trip count
    from the inventory adjustment.
    """

    name: str = "HISTORICAL_TRIP_SAMPLING"

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
        """Append observed trips for this period to ``state.in_transit``."""
        flows = _period_flows(resolved, period)
        if flows is None:
            return PhaseResult.empty(state)

        n = len(flows)
        new_trips = pd.DataFrame({
            "shipment_id": [
                f"organic_trip_{period.period_index}_{i}" for i in range(n)
            ],
            "source_id": flows["source_id"].to_numpy(),
            "target_id": flows["target_id"].to_numpy(),
            "commodity_category": flows["commodity_category"].to_numpy(),
            "quantity": flows["quantity"].to_numpy(),
            "resource_id": [None] * n,
            "departure_period": [period.period_index] * n,
            "arrival_period": [period.period_index] * n,
        })

        if state.in_transit.empty:
            new_in_transit = new_trips
        else:
            new_in_transit = pd.concat(
                [state.in_transit, new_trips], ignore_index=True,
            )

        return PhaseResult(state=state.with_in_transit(new_in_transit))


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

        # 2. Aggregate arriving quantities and add to target inventory.
        arrival_totals = to_inventory_delta(arriving, facility_col="target_id")
        new_inv = apply_delta(state.inventory, arrival_totals, op="add")

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
        events = {"flow_events": flow_events} if not flow_events.empty else {}
        return PhaseResult(state=new_state, events=events)


class DockCapacityPhase:
    """Enforce facility storage capacity after arrivals.

    Reads ``resolved.attributes["operation_capacity"]`` filtered by
    ``operation_type == "storage"``: a per-facility, per-commodity upper bound
    on inventory.  After arrivals have landed, any facility whose inventory
    exceeds capacity is clipped down to capacity, and the excess is recorded
    in ``simulation_dock_blocking_log``.

    Pipeline contract: must be placed after :class:`ArrivalsPhase` so it
    operates on post-arrival inventory.

    The phase is a no-op when:

    - ``resolved.attributes`` lacks ``"operation_capacity"`` (e.g., a domain
      that does not model storage limits).
    - The capacity table is empty or has no rows with
      ``operation_type == "storage"``.
    - Every facility either has no capacity row (treated as unbounded) or
      already satisfies its capacity.

    For historical replay, observed inventory should not exceed capacity, so
    this phase typically logs nothing.  When it does fire, that is a
    meaningful data-quality signal.

    Log columns (interpretation):

    - ``incoming``  — post-arrivals quantity that approached the dock.
    - ``accepted``  — quantity that fit (= ``min(incoming, capacity)``).
    - ``blocked``   — quantity that could not fit (= ``incoming - accepted``).
    """

    name: str = "DOCK_CAPACITY"

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
        """Clip inventory to storage capacity; log overflow as dock blocking."""
        if "operation_capacity" not in resolved.attributes:
            return PhaseResult.empty(state)

        cap_data = resolved.attributes.get("operation_capacity").data
        storage = cap_data.loc[
            cap_data["operation_type"] == "storage",
            ["facility_id", "commodity_category", "capacity"],
        ]
        if storage.empty:
            return PhaseResult.empty(state)

        merged = state.inventory.merge(
            storage, on=["facility_id", "commodity_category"], how="left",
        )
        bounded = merged["capacity"].notna()
        merged["accepted"] = merged["quantity"].where(
            ~bounded,
            np.minimum(merged["quantity"], merged["capacity"]),
        )
        merged["blocked"] = merged["quantity"] - merged["accepted"]

        new_inv = merged[["facility_id", "commodity_category"]].copy()
        new_inv["quantity"] = merged["accepted"]

        overflow = merged[merged["blocked"] > 0]
        dock_log = pd.DataFrame({
            "facility_id": overflow["facility_id"].values,
            "commodity_category": overflow["commodity_category"].values,
            "incoming": overflow["quantity"].values,
            "accepted": overflow["accepted"].values,
            "blocked": overflow["blocked"].values,
        })

        events = {"dock_blocking": dock_log} if not dock_log.empty else {}
        return PhaseResult(state=state.with_inventory(new_inv), events=events)
