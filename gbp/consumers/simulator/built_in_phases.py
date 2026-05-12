"""Built-in (universal) phases: DemandPhase and ArrivalsPhase.

These phases implement domain-agnostic logic that applies to any commodity
network.  Domain-specific phases (DispatchPhase, SupplyPhase, TransformPhase)
live in separate modules.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Literal

import numpy as np
import pandas as pd

from gbp.consumers.simulator._period_helpers import period_duration_hours
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

    Parameters
    ----------
    schedule
        Optional execution schedule.  Defaults to every period.
    """

    name: str = "DEMAND"

    def __init__(self, schedule: Schedule | None = None) -> None:
        """Initialise with an optional schedule."""
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
        period_demand = resolved.demand[resolved.demand["period_id"] == period.period_id].copy()

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
        flow_events = pd.DataFrame(
            {
                "source_id": "EXT",
                "target_id": consumed["facility_id"].values,
                "commodity_category": consumed["commodity_category"].values,
                "modal_type": None,
                "quantity": consumed["fulfilled"].values,
                "resource_id": None,
            }
        )

        # 6. Unmet demand (only rows with deficit > 0)
        unmet_rows = merged[merged["deficit"] > 0]
        unmet_demand = pd.DataFrame(
            {
                "facility_id": unmet_rows["facility_id"].values,
                "commodity_category": unmet_rows["commodity_category"].values,
                "requested": unmet_rows["demand_qty"].values,
                "fulfilled": unmet_rows["fulfilled"].values,
                "deficit": unmet_rows["deficit"].values,
            }
        )

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
    """Return observed_flow rows for *period*, or ``None`` if empty.

    Parameters
    ----------
    resolved
        Resolved model carrying ``observed_flow``.
    period
        Period descriptor to filter by.

    Returns
    -------
    pd.DataFrame or None
        Filtered rows, or ``None`` when no flows exist for the period.
    """
    if resolved.observed_flow.empty:
        return None
    flows = resolved.observed_flow[resolved.observed_flow["period_id"] == period.period_id]
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

    Parameters
    ----------
    schedule
        Optional execution schedule.  Defaults to every period.
    """

    name: str = "ORGANIC_DEPARTURE"

    def __init__(self, schedule: Schedule | None = None) -> None:
        """Initialise with an optional schedule."""
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

        flow_events = pd.DataFrame(
            {
                col: (flows[col].values if col in flows.columns else [None] * len(flows))
                for col in _FLOW_EVENT_COLUMNS
            }
        )

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

    Parameters
    ----------
    schedule
        Optional execution schedule.  Defaults to every period.
    """

    name: str = "ORGANIC_ARRIVAL"

    def __init__(self, schedule: Schedule | None = None) -> None:
        """Initialise with an optional schedule."""
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

    Parameters
    ----------
    schedule
        Optional execution schedule.  Defaults to every period.
    """

    name: str = "ORGANIC_FLOW"

    def __init__(self, schedule: Schedule | None = None) -> None:
        """Initialise with an optional schedule."""
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

    Parameters
    ----------
    schedule
        Optional execution schedule.  Defaults to every period.
    """

    name: str = "HISTORICAL_LATENT_DEMAND"

    def __init__(self, schedule: Schedule | None = None) -> None:
        """Initialise with an optional schedule."""
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
            .rename(
                columns={
                    "source_id": "facility_id",
                    "quantity": "latent_departures",
                }
            )
        )
        arrivals = (
            flows.groupby(["target_id", "commodity_category"], as_index=False)["quantity"]
            .sum()
            .rename(
                columns={
                    "target_id": "facility_id",
                    "quantity": "latent_arrivals",
                }
            )
        )

        latent = departures.merge(
            arrivals,
            on=["facility_id", "commodity_category"],
            how="outer",
        )
        latent["latent_departures"] = latent["latent_departures"].fillna(0.0)
        latent["latent_arrivals"] = latent["latent_arrivals"].fillna(0.0)
        latent = latent.sort_values(
            ["facility_id", "commodity_category"],
            kind="stable",
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

    Parameters
    ----------
    schedule
        Optional execution schedule.  Defaults to every period.
    """

    name: str = "HISTORICAL_OD_STRUCTURE"

    def __init__(self, schedule: Schedule | None = None) -> None:
        """Initialise with an optional schedule."""
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
                ["source_id", "target_id", "commodity_category"],
                as_index=False,
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
            origin_total,
            on=["source_id", "commodity_category"],
            how="left",
        )
        od["probability"] = od["joint"] / od["origin_total"]
        od = od[["source_id", "target_id", "commodity_category", "probability"]]
        od = od.sort_values(
            ["source_id", "commodity_category", "target_id"],
            kind="stable",
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

    Parameters
    ----------
    mode
        ``"permissive"`` for historical replay (no clipping),
        ``"strict"`` for predictive scenarios (enforce inventory >= 0).
        Defaults to ``"permissive"``.
    schedule
        Optional execution schedule.  Defaults to every period.

    Raises
    ------
    ValueError
        If *mode* is not one of ``"permissive"`` or ``"strict"``.
    """

    name: str = "DEPARTURE_PHYSICS"

    def __init__(
        self,
        mode: Literal["permissive", "strict"] = "permissive",
        schedule: Schedule | None = None,
    ) -> None:
        """Initialise the phase."""
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
            state.inventory,
            dep,
            value_col="latent_departures",
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
        lost_log = pd.DataFrame(
            {
                "facility_id": lost_rows["facility_id"].values,
                "commodity_category": lost_rows["commodity_category"].values,
                "latent": lost_rows["latent_departures"].values,
                "realized": lost_rows["realized"].values,
                "lost": lost_rows["lost"].values,
            }
        )

        new_state = state.with_inventory(new_inv).with_intermediates(
            realized_departures=realized_dep
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

    When ``use_durations`` is ``True`` (the default) and the per-period
    ``observed_flow`` slice carries a ``duration_hours`` column, the
    arrival period is computed as
    ``period_index + ceil(duration_hours / period_duration_hours)`` per
    row.  Rows with null ``duration_hours`` fall back to same-period
    delivery (``tau = 0``), preserving the legacy zero-tau semantics for
    historical sources that do not record trip end times.

    Set ``use_durations=False`` to force same-period delivery for every
    row, regardless of the column's presence — matches the pre-extension
    behaviour for callers that explicitly opt out of the duration logic.

    Parameters
    ----------
    use_durations
        When ``True`` (default), compute arrival periods from
        ``duration_hours`` if available.  When ``False``, force
        same-period delivery for every row.
    schedule
        Optional execution schedule.  Defaults to every period.
    """

    name: str = "HISTORICAL_TRIP_SAMPLING"

    def __init__(
        self,
        use_durations: bool = True,
        schedule: Schedule | None = None,
    ) -> None:
        """Initialise with an optional duration-mode flag and schedule."""
        self._use_durations = use_durations
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
        if self._use_durations and "duration_hours" in flows.columns:
            period_dur_h = period_duration_hours(resolved)
            duration_filled = flows["duration_hours"].fillna(0.0).to_numpy(dtype=float)
            # ``floor`` matches the doc's "if t_arr <= t+1: same period"
            # semantics: trips with duration <= one period stay in-period;
            # only durations exceeding a full period bump the arrival forward.
            # ``ceil`` would over-count by one for every intra-period trip.
            tau_periods = np.floor(duration_filled / period_dur_h).astype(int)
            arrival_periods = period.period_index + tau_periods
        else:
            arrival_periods = np.full(n, period.period_index, dtype=int)

        new_trips = pd.DataFrame(
            {
                "shipment_id": [f"organic_trip_{period.period_index}_{i}" for i in range(n)],
                "source_id": flows["source_id"].to_numpy(),
                "target_id": flows["target_id"].to_numpy(),
                "commodity_category": flows["commodity_category"].to_numpy(),
                "quantity": flows["quantity"].to_numpy(),
                "resource_id": [None] * n,
                "departure_period": [period.period_index] * n,
                "arrival_period": arrival_periods,
            }
        )

        if state.in_transit.empty:
            new_in_transit = new_trips
        else:
            new_in_transit = pd.concat(
                [state.in_transit, new_trips],
                ignore_index=True,
            )

        return PhaseResult(state=state.with_in_transit(new_in_transit))


class ArrivalsPhase:
    """Process shipments arriving at their destination in the current period.

    Filters ``state.in_transit`` for shipments whose ``arrival_period`` matches
    the current ``period.period_index``, transfers commodity into target facility
    inventory, and updates resource statuses from IN_TRANSIT to AVAILABLE.

    Parameters
    ----------
    schedule
        Optional execution schedule.  Defaults to every period.
    """

    name: str = "ARRIVALS"

    def __init__(self, schedule: Schedule | None = None) -> None:
        """Initialise with an optional schedule."""
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
        arriving_resources = arriving[arriving["resource_id"].notna()][
            ["resource_id", "target_id"]
        ].drop_duplicates(subset=["resource_id"])

        if not arriving_resources.empty:
            res_map = arriving_resources.set_index("resource_id")["target_id"]
            mask = resources["resource_id"].isin(res_map.index)
            resources.loc[mask, "status"] = ResourceStatus.AVAILABLE.value
            resources.loc[mask, "available_at_period"] = None
            resources.loc[mask, "current_facility_id"] = resources.loc[mask, "resource_id"].map(
                res_map
            )

        # 5. Build flow events (one row per arriving shipment)
        flow_events = pd.DataFrame(
            {
                "source_id": arriving["source_id"].values,
                "target_id": arriving["target_id"].values,
                "commodity_category": arriving["commodity_category"].values,
                "modal_type": arriving.get("modal_type", pd.Series(dtype=str)).values
                if "modal_type" in arriving.columns
                else [None] * len(arriving),
                "quantity": arriving["quantity"].values,
                "resource_id": arriving["resource_id"].values,
            }
        )

        # 6. Return updated state
        new_state = (
            state.with_inventory(new_inv).with_in_transit(remaining).with_resources(resources)
        )
        events = {"flow_events": flow_events} if not flow_events.empty else {}
        return PhaseResult(state=new_state, events=events)


class LatentDemandInflatorPhase:
    """Scale ``state.intermediates["latent_demand"]`` by a multiplier.

    Pipeline placement: insert **after** :class:`HistoricalLatentDemandPhase`
    and **before** :class:`DeparturePhysicsPhase` (mode ``"strict"``).  The
    inflated marginals then cause ``DeparturePhysicsPhase`` to record
    ``lost_demand > 0`` even when historical inventory would normally satisfy
    the baseline, making the rebalancing benefit measurable.

    Semantic: multiplies the historical latent demand to create artificial
    demand pressure for experiments.  Both ``latent_departures`` and
    ``latent_arrivals`` are scaled to maintain supply/demand symmetry within
    each period.

    Warning: with ``multiplier=1.0`` (the default) this phase is a pure
    identity operation — intermediates are unchanged and no events are emitted.
    That is intentional: it makes the phase safe to insert unconditionally
    into a pipeline without changing existing behaviour.

    Parameters
    ----------
    multiplier
        Either a scalar ``float`` applied to every facility row,
        or a ``dict[str, float]`` mapping ``facility_id`` to a per-facility
        factor.  Facilities absent from the dict receive an implicit
        multiplier of ``1.0``.  Defaults to ``1.0``.
    schedule
        Optional execution schedule.  Defaults to every period.
    """

    name: str = "LATENT_DEMAND_INFLATOR"

    def __init__(
        self,
        multiplier: float | dict[str, float] = 1.0,
        schedule: Schedule | None = None,
    ) -> None:
        """Initialise with an optional multiplier and schedule."""
        self._multiplier = multiplier
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
        """Scale ``latent_demand`` in intermediates by the configured multiplier.

        Parameters
        ----------
        state
            Current simulation state.
        resolved
            Resolved model data (unused; required by the Phase protocol).
        period
            Current period descriptor (unused; required by the Phase protocol).

        Returns
        -------
        PhaseResult
            Updated state with scaled latent demand.  No events are emitted —
            this phase only mutates intermediate state.
        """
        latent = state.intermediates.get("latent_demand")
        if latent is None or latent.empty:
            return PhaseResult.empty(state)

        latent = latent.copy()

        if isinstance(self._multiplier, dict):
            factors = latent["facility_id"].map(self._multiplier).fillna(1.0)
            latent["latent_departures"] = latent["latent_departures"] * factors
            latent["latent_arrivals"] = latent["latent_arrivals"] * factors
        else:
            latent["latent_departures"] = latent["latent_departures"] * self._multiplier
            latent["latent_arrivals"] = latent["latent_arrivals"] * self._multiplier

        new_state = state.with_intermediates(latent_demand=latent)
        return PhaseResult(state=new_state, events={})


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

    Parameters
    ----------
    schedule
        Optional execution schedule.  Defaults to every period.
    """

    name: str = "DOCK_CAPACITY"

    def __init__(self, schedule: Schedule | None = None) -> None:
        """Initialise with an optional schedule."""
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
            storage,
            on=["facility_id", "commodity_category"],
            how="left",
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
        dock_log = pd.DataFrame(
            {
                "facility_id": overflow["facility_id"].values,
                "commodity_category": overflow["commodity_category"].values,
                "incoming": overflow["quantity"].values,
                "accepted": overflow["accepted"].values,
                "blocked": overflow["blocked"].values,
            }
        )

        events = {"dock_blocking": dock_log} if not dock_log.empty else {}
        return PhaseResult(state=state.with_inventory(new_inv), events=events)


class EndOfPeriodDeficitPhase:
    """Record end-of-period negative inventory as lost demand.

    Pairs with :class:`DeparturePhysicsPhase` ``mode="permissive"`` to form an
    "end-of-period clip" semantics: departures are subtracted without start-of-
    period clipping, arrivals are then added, and only the residual negative
    balance at the end of the period is treated as unmet demand.  This is the
    conservative complement to ``mode="strict"`` (start-of-period clip): strict
    over-reports lost demand by ignoring same-period arrivals; this phase
    under-reports it by assuming intra-period arrivals are interleaved with
    departures.  Truth lies between the two bounds.

    Pipeline placement: after :class:`ArrivalsPhase` and
    :class:`OverflowRedirectPhase`, before :class:`DispatchPhase` so the
    rebalancer sees clipped (non-negative) inventory.

    On the canonical historical replay (no inflator, ``multiplier=1.0``) the
    end-of-period inventory is non-negative by construction of the source
    data, so this phase is a no-op.  Deficits arise only in treatment
    scenarios where :class:`LatentDemandInflatorPhase` scales the latent
    marginals above what physical supply can cover.

    Parameters
    ----------
    tolerance
        Absolute floor below which a negative inventory is
        treated as floating-point noise rather than a real deficit.
        Defaults to ``1e-9``.
    schedule
        Optional execution schedule.  Defaults to every period.

    Notes
    -----
    Conservation: bikes counted as "lost" never actually left the source
    station — the customer did not find a bike.  To keep
    ``inventory + in_transit`` consistent, the phase removes a matching
    quantity from current-period shipments departing the deficit facility,
    in DataFrame order.  When same-period trips have already been delivered
    by ``ArrivalsPhase`` (``tau == 0``), those shipments are no longer in
    ``in_transit`` and cannot be reduced; the residual mismatch surfaces
    through :class:`InvariantCheckPhase` (use ``fail_on_violation=False``
    in this mode).
    """

    name: str = "END_OF_PERIOD_DEFICIT"

    def __init__(
        self,
        tolerance: float = 1e-9,
        schedule: Schedule | None = None,
    ) -> None:
        """Initialise with optional tolerance and schedule."""
        self._tolerance = tolerance
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
        """Detect end-of-period deficits, record them, clip inventory to zero."""
        if state.inventory.empty:
            return PhaseResult.empty(state)

        deficit_mask = state.inventory["quantity"] < -self._tolerance
        if not deficit_mask.any():
            return PhaseResult.empty(state)

        # --- build deficits table (one row per facility+commodity) ----------
        new_inventory = state.inventory.copy()
        deficits = new_inventory.loc[
            deficit_mask, ["facility_id", "commodity_category", "quantity"]
        ].copy()
        deficits["deficit"] = -deficits["quantity"]

        # clip all deficit inventory rows to zero in one shot
        new_inventory.loc[deficit_mask, "quantity"] = 0.0

        # --- build lost_demand DataFrame via merge with latent demand ------
        latent = state.intermediates.get("latent_demand")
        if latent is not None and not latent.empty:
            latent_cols = latent[["facility_id", "commodity_category", "latent_departures"]]
            lost = deficits[["facility_id", "commodity_category", "deficit"]].merge(
                latent_cols,
                on=["facility_id", "commodity_category"],
                how="left",
            )
            lost = lost.rename(columns={"latent_departures": "latent", "deficit": "lost"})
            lost["realized"] = lost["latent"] - lost["lost"]
        else:
            lost = deficits[["facility_id", "commodity_category", "deficit"]].rename(
                columns={"deficit": "lost"},
            )
            lost["latent"] = float("nan")
            lost["realized"] = float("nan")

        # --- vectorised in-transit reduction --------------------------------
        new_in_transit = state.in_transit.copy()
        if not new_in_transit.empty:
            new_in_transit = self._reduce_in_transit_vec(
                new_in_transit,
                deficits,
                period.period_index,
                self._tolerance,
            )

        new_state = state.with_inventory(new_inventory.reset_index(drop=True)).with_in_transit(
            new_in_transit
        )
        return PhaseResult(
            state=new_state,
            events={
                "lost_demand": lost[
                    ["facility_id", "commodity_category", "latent", "realized", "lost"]
                ].reset_index(drop=True),
            },
        )

    @staticmethod
    def _reduce_in_transit_vec(
        in_transit: pd.DataFrame,
        deficits: pd.DataFrame,
        period_index: int,
        tolerance: float,
    ) -> pd.DataFrame:
        """Vectorised best-effort cancellation of current-period shipments.

        For each deficit ``(facility_id, commodity_category)``, reduces
        matching shipment quantities in DataFrame order until the deficit is
        covered or no candidates remain.

        Parameters
        ----------
        in_transit
            In-transit DataFrame (caller-owned copy, will not be mutated).
        deficits
            DataFrame with columns ``facility_id``, ``commodity_category``,
            ``deficit`` (positive quantities to cancel).
        period_index
            Current period index — only shipments with
            ``departure_period == period_index`` are candidates.
        tolerance
            Rows with ``quantity <= tolerance`` are dropped from the result.

        Returns
        -------
        pd.DataFrame
            Updated in-transit DataFrame with shipment quantities reduced
            and near-zero rows removed.
        """
        cand_mask = in_transit["departure_period"] == period_index
        if not cand_mask.any():
            return in_transit

        # tag original positions so DataFrame order is preserved
        candidates = in_transit.loc[cand_mask].copy()
        candidates["_orig_idx"] = candidates.index

        merged = candidates.merge(
            deficits[["facility_id", "commodity_category", "deficit"]],
            left_on=["source_id", "commodity_category"],
            right_on=["facility_id", "commodity_category"],
            how="inner",
        )
        if merged.empty:
            return in_transit

        # preserve original DataFrame order for deterministic cancellation
        merged = merged.sort_values("_orig_idx")

        # cumsum within each (source, commodity) group
        grouped_cum = merged.groupby(
            ["source_id", "commodity_category"],
            sort=False,
        )["quantity"].cumsum()
        prev_cumsum = grouped_cum - merged["quantity"]

        # per-row reduction: min(qty, max(0, deficit - cumsum_before_this_row))
        reduction = np.minimum(
            merged["quantity"].to_numpy(),
            np.maximum(0.0, merged["deficit"].to_numpy() - prev_cumsum.to_numpy()),
        )

        # aggregate reductions back to original indices
        reductions = pd.Series(reduction, index=merged["_orig_idx"]).groupby(level=0).sum()

        result = in_transit.copy()
        result.loc[reductions.index, "quantity"] -= reductions
        result = result[result["quantity"] > tolerance].reset_index(drop=True)
        return result


class InvariantViolationError(RuntimeError):
    """Raised on per-commodity conservation violation.

    Emitted by :class:`InvariantCheckPhase` when ``fail_on_violation=True``
    and the per-commodity baseline does not match the current state.
    """


class InvariantCheckPhase:
    """Assert per-commodity conservation across the simulation.

    For every commodity, the total stock plus in-transit quantity must equal
    the baseline established at construction time (or on first ``execute``
    when ``baseline=None``; see ADR Sec. 7.4).  A violation either raises
    :class:`InvariantViolationError` (default) or emits a row to
    ``simulation_invariant_violation_log``.

    ORDERING CONTRACT: Must run last in the period.  Other phases mutate
    inventory and in_transit; running this phase mid-period would assert
    against a transient state.

    Per ADR Sec. 7.7: per-commodity baseline tracks ONLY commodities present
    in the initial inventory snapshot (i.e., commodities that appear with a
    non-zero baseline).  Commodities appearing later in ``observed_flow``
    but absent from ``inventory_initial`` are EXCLUDED from invariant
    tracking and never trigger spurious violations.

    Per ADR Sec. 7.4: when ``baseline=None`` the first ``execute`` captures
    per-commodity totals into ``state.intermediates["invariant_baseline"]``
    and memoises them on ``self`` so subsequent periods (where
    ``intermediates`` has been wiped by ``advance_period``) still see the
    captured value.  No log row is emitted on capture.

    Parameters
    ----------
    baseline
        Optional per-commodity baseline ``dict[str, float]``.
        When omitted the first ``execute`` captures the current
        state as the baseline.
    fail_on_violation
        When ``True`` (default for canonical replay), violations raise
        :class:`InvariantViolationError`.  When ``False`` (treatment
        scenarios), violations are logged into
        ``simulation_invariant_violation_log`` and the run continues.
    tolerance
        Absolute tolerance for floating-point comparison.
        Defaults to ``1e-9``.
    schedule
        Optional execution schedule.  Defaults to every period.
    """

    name: str = "INVARIANT_CHECK"

    def __init__(
        self,
        *,
        baseline: Mapping[str, float] | None = None,
        fail_on_violation: bool = True,
        tolerance: float = 1e-9,
        schedule: Schedule | None = None,
    ) -> None:
        """Initialise the phase."""
        self._baseline: dict[str, float] | None = None if baseline is None else dict(baseline)
        self._fail_on_violation = fail_on_violation
        self._tolerance = tolerance
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
        """Capture-or-assert the per-commodity conservation invariant."""
        current = _per_commodity_total(state)

        if self._baseline is None:
            self._baseline = {k: float(v) for k, v in current.items()}
            return PhaseResult(
                state=state.with_intermediates(
                    invariant_baseline=dict(self._baseline),
                ),
                events={},
            )

        violations: list[dict[str, object]] = []
        for commodity, baseline in self._baseline.items():
            observed = float(current.get(commodity, 0.0))
            delta = observed - baseline
            if abs(delta) > self._tolerance:
                violations.append(
                    {
                        "commodity_category": commodity,
                        "baseline": baseline,
                        "current": observed,
                        "delta": delta,
                    }
                )

        if not violations:
            return PhaseResult.empty(state)

        if self._fail_on_violation:
            offending = ", ".join(
                f"{v['commodity_category']} "
                f"(baseline={v['baseline']}, current={v['current']}, "
                f"delta={v['delta']})"
                for v in violations
            )
            msg = f"Invariant violated at period {period.period_id}: {offending}"
            raise InvariantViolationError(msg)

        violation_df = pd.DataFrame(violations)
        return PhaseResult(
            state=state,
            events={"invariant_violation": violation_df},
        )


def _per_commodity_total(state: SimulationState) -> dict[str, float]:
    """Sum ``state.inventory + state.in_transit`` per commodity_category.

    Parameters
    ----------
    state
        Current simulation state.

    Returns
    -------
    dict[str, float]
        Mapping from commodity category to total quantity.
    """
    if state.inventory.empty:
        inv_totals = pd.Series(dtype=float)
    else:
        inv_totals = state.inventory.groupby("commodity_category")["quantity"].sum()
    if state.in_transit.empty:
        transit_totals = pd.Series(dtype=float)
    else:
        transit_totals = state.in_transit.groupby("commodity_category")["quantity"].sum()
    combined = inv_totals.add(transit_totals, fill_value=0.0)
    return {str(k): float(v) for k, v in combined.items()}
