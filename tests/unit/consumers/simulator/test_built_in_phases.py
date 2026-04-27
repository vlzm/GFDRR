"""Tests for DemandPhase, ArrivalsPhase, and Organic*Phase."""
# ruff: noqa: D102

from __future__ import annotations

from datetime import date

import pandas as pd
from pandas.testing import assert_frame_equal

from gbp.consumers.simulator.built_in_phases import (
    ArrivalsPhase,
    DemandPhase,
    OrganicArrivalPhase,
    OrganicDeparturePhase,
    OrganicFlowPhase,
)
from gbp.consumers.simulator.config import EnvironmentConfig
from gbp.consumers.simulator.engine import Environment
from gbp.consumers.simulator.phases import Phase, Schedule
from gbp.consumers.simulator.state import PeriodRow, init_state
from gbp.core.model import ResolvedModelData


def _make_period(period_index: int = 0, period_id: str = "p0") -> PeriodRow:
    return PeriodRow(
        Index=0,
        period_id=period_id,
        planning_horizon_id="h1",
        segment_index=0,
        period_index=period_index,
        period_type="day",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
    )


class TestDemandPhaseContract:
    """DemandPhase satisfies the Phase protocol and delegates scheduling."""

    def test_is_phase(self) -> None:
        assert isinstance(DemandPhase(), Phase)

    def test_name(self) -> None:
        assert DemandPhase().name == "DEMAND"

    def test_should_run_delegates_to_schedule(self) -> None:
        phase = DemandPhase(schedule=Schedule.every_n(2))
        assert phase.should_run(_make_period(period_index=0))
        assert not phase.should_run(_make_period(period_index=1))
        assert phase.should_run(_make_period(period_index=2))


class TestArrivalsPhaseContract:
    """ArrivalsPhase satisfies the Phase protocol and delegates scheduling."""

    def test_is_phase(self) -> None:
        assert isinstance(ArrivalsPhase(), Phase)

    def test_name(self) -> None:
        assert ArrivalsPhase().name == "ARRIVALS"


class TestDemandPhaseExecute:
    """DemandPhase.execute behaviour (enable after implementation)."""

    def test_demand_reduces_inventory(
        self, resolved_model: ResolvedModelData
    ) -> None:
        state = init_state(resolved_model)
        phase = DemandPhase()
        # resolved_model has demand at s1 for period p0: quantity=1.0
        result = phase.execute(state, resolved_model, _make_period(0, "p0"))

        new_inv = result.state.inventory
        s1_qty = new_inv.loc[
            new_inv["facility_id"] == "s1", "quantity"
        ].iloc[0]
        assert s1_qty == 12.0 - 1.0  # initial 12, demand 1

    def test_unmet_demand_logged(
        self, resolved_model: ResolvedModelData
    ) -> None:
        state = init_state(resolved_model)
        # Set s1 inventory to 0 so demand is unmet
        inv = state.inventory.copy()
        inv.loc[inv["facility_id"] == "s1", "quantity"] = 0.0
        state = state.with_inventory(inv)

        phase = DemandPhase()
        result = phase.execute(state, resolved_model, _make_period(0, "p0"))

        assert not result.unmet_demand.empty
        assert result.unmet_demand.iloc[0]["deficit"] > 0

    def test_no_demand_returns_empty(
        self, resolved_model: ResolvedModelData
    ) -> None:
        state = init_state(resolved_model)
        phase = DemandPhase()
        # period p99 has no demand
        result = phase.execute(state, resolved_model, _make_period(99, "p99"))
        assert result.state is state


class TestArrivalsPhaseExecute:
    """ArrivalsPhase.execute behaviour (enable after implementation)."""

    def test_arriving_shipment_transfers_to_inventory(
        self, resolved_model: ResolvedModelData
    ) -> None:
        state = init_state(resolved_model)
        # Add a shipment arriving at period 0
        transit = pd.DataFrame(
            {
                "shipment_id": ["shp_001"],
                "source_id": ["d1"],
                "target_id": ["s1"],
                "commodity_category": ["working_bike"],
                "quantity": [5.0],
                "resource_id": ["rebalancing_truck_d1_0"],
                "departure_period": [0],
                "arrival_period": [0],
            }
        )
        state = state.with_in_transit(transit)

        phase = ArrivalsPhase()
        result = phase.execute(state, resolved_model, _make_period(0, "p0"))

        # s1 inventory should increase by 5
        new_inv = result.state.inventory
        s1_qty = new_inv.loc[
            new_inv["facility_id"] == "s1", "quantity"
        ].iloc[0]
        assert s1_qty == 12.0 + 5.0

        # in_transit should be empty
        assert result.state.in_transit.empty

    def test_non_arriving_shipment_stays(
        self, resolved_model: ResolvedModelData
    ) -> None:
        state = init_state(resolved_model)
        transit = pd.DataFrame(
            {
                "shipment_id": ["shp_001"],
                "source_id": ["d1"],
                "target_id": ["s1"],
                "commodity_category": ["working_bike"],
                "quantity": [5.0],
                "resource_id": ["rebalancing_truck_d1_0"],
                "departure_period": [0],
                "arrival_period": [2],
            }
        )
        state = state.with_in_transit(transit)

        phase = ArrivalsPhase()
        result = phase.execute(state, resolved_model, _make_period(0, "p0"))

        assert len(result.state.in_transit) == 1


# ---------------------------------------------------------------------------
# OrganicDeparturePhase / OrganicArrivalPhase
# ---------------------------------------------------------------------------


class TestOrganicDeparturePhaseContract:
    """OrganicDeparturePhase satisfies Phase protocol."""

    def test_is_phase(self) -> None:
        assert isinstance(OrganicDeparturePhase(), Phase)

    def test_name(self) -> None:
        assert OrganicDeparturePhase().name == "ORGANIC_DEPARTURE"

    def test_should_run_delegates_to_schedule(self) -> None:
        phase = OrganicDeparturePhase(schedule=Schedule.every_n(2))
        assert phase.should_run(_make_period(period_index=0))
        assert not phase.should_run(_make_period(period_index=1))


class TestOrganicArrivalPhaseContract:
    """OrganicArrivalPhase satisfies Phase protocol."""

    def test_is_phase(self) -> None:
        assert isinstance(OrganicArrivalPhase(), Phase)

    def test_name(self) -> None:
        assert OrganicArrivalPhase().name == "ORGANIC_ARRIVAL"


class TestOrganicDeparturePhaseExecute:
    """OrganicDeparturePhase subtracts outflow from sources."""

    def test_departure_reduces_source_inventory(
        self, resolved_model_with_obs: ResolvedModelData,
    ) -> None:
        state = init_state(resolved_model_with_obs)
        phase = OrganicDeparturePhase()
        # p0 observed_flow: s1→s2 qty=2, s2→s1 qty=1
        result = phase.execute(
            state, resolved_model_with_obs, _make_period(0, "p0"),
        )
        inv = result.state.inventory
        s1_qty = inv.loc[inv["facility_id"] == "s1", "quantity"].iloc[0]
        s2_qty = inv.loc[inv["facility_id"] == "s2", "quantity"].iloc[0]
        # s1: 8 - 2 = 6, s2: 12 - 1 = 11
        assert s1_qty == 6.0
        assert s2_qty == 11.0

    def test_departure_emits_flow_events(
        self, resolved_model_with_obs: ResolvedModelData,
    ) -> None:
        state = init_state(resolved_model_with_obs)
        phase = OrganicDeparturePhase()
        result = phase.execute(
            state, resolved_model_with_obs, _make_period(0, "p0"),
        )
        assert not result.flow_events.empty
        assert "source_id" in result.flow_events.columns
        assert "target_id" in result.flow_events.columns

    def test_no_observed_flow_returns_empty(
        self, resolved_model: ResolvedModelData,
    ) -> None:
        state = init_state(resolved_model)
        phase = OrganicDeparturePhase()
        result = phase.execute(state, resolved_model, _make_period(0, "p0"))
        assert result.state is state


class TestOrganicArrivalPhaseExecute:
    """OrganicArrivalPhase adds inflow to targets."""

    def test_arrival_increases_target_inventory(
        self, resolved_model_with_obs: ResolvedModelData,
    ) -> None:
        state = init_state(resolved_model_with_obs)
        phase = OrganicArrivalPhase()
        # p0 observed_flow: s1→s2 qty=2, s2→s1 qty=1
        result = phase.execute(
            state, resolved_model_with_obs, _make_period(0, "p0"),
        )
        inv = result.state.inventory
        s1_qty = inv.loc[inv["facility_id"] == "s1", "quantity"].iloc[0]
        s2_qty = inv.loc[inv["facility_id"] == "s2", "quantity"].iloc[0]
        # s1: 8 + 1 = 9, s2: 12 + 2 = 14
        assert s1_qty == 9.0
        assert s2_qty == 14.0

    def test_arrival_emits_no_flow_events(
        self, resolved_model_with_obs: ResolvedModelData,
    ) -> None:
        state = init_state(resolved_model_with_obs)
        phase = OrganicArrivalPhase()
        result = phase.execute(
            state, resolved_model_with_obs, _make_period(0, "p0"),
        )
        assert result.flow_events.empty


class TestOrganicPhaseParity:
    """Departure+Arrival produces identical results to OrganicFlowPhase."""

    def test_split_equals_combined_single_period(
        self, resolved_model_with_obs: ResolvedModelData,
    ) -> None:
        period = _make_period(0, "p0")
        state = init_state(resolved_model_with_obs)

        # Combined
        combined = OrganicFlowPhase()
        combined_result = combined.execute(
            state, resolved_model_with_obs, period,
        )

        # Split
        dep = OrganicDeparturePhase()
        arr = OrganicArrivalPhase()
        dep_result = dep.execute(state, resolved_model_with_obs, period)
        arr_result = arr.execute(
            dep_result.state, resolved_model_with_obs, period,
        )

        # Inventory must match exactly
        assert_frame_equal(
            combined_result.state.inventory.reset_index(drop=True),
            arr_result.state.inventory.reset_index(drop=True),
        )

        # Flow events must match exactly
        assert_frame_equal(
            combined_result.flow_events.reset_index(drop=True),
            dep_result.flow_events.reset_index(drop=True),
        )

    def test_full_run_parity(
        self, resolved_model_with_obs: ResolvedModelData,
    ) -> None:
        """Full simulation: split phases produce same logs as combined."""
        combined_log = Environment(
            resolved_model_with_obs,
            EnvironmentConfig(
                phases=[OrganicFlowPhase()], scenario_id="combined",
            ),
        ).run()

        split_log = Environment(
            resolved_model_with_obs,
            EnvironmentConfig(
                phases=[OrganicDeparturePhase(), OrganicArrivalPhase()],
                scenario_id="split",
            ),
        ).run()

        combined_tables = combined_log.to_dataframes()
        split_tables = split_log.to_dataframes()

        # Inventory logs must match
        assert_frame_equal(
            combined_tables["simulation_inventory_log"].reset_index(drop=True),
            split_tables["simulation_inventory_log"].reset_index(drop=True),
        )

        # Flow quantities must match (phase_name differs, so compare aggregates)
        flow_keys = ["period_id", "source_id", "target_id", "commodity_category"]
        combined_flow = (
            combined_tables["simulation_flow_log"]
            .groupby(flow_keys, as_index=False)["quantity"].sum()
            .sort_values(flow_keys)
            .reset_index(drop=True)
        )
        split_flow = (
            split_tables["simulation_flow_log"]
            .groupby(flow_keys, as_index=False)["quantity"].sum()
            .sort_values(flow_keys)
            .reset_index(drop=True)
        )
        assert_frame_equal(combined_flow, split_flow)
