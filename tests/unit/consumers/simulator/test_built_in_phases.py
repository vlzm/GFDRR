"""Tests for DemandPhase and ArrivalsPhase."""
# ruff: noqa: D102

from __future__ import annotations

from datetime import date

import pandas as pd

from gbp.consumers.simulator.built_in_phases import ArrivalsPhase, DemandPhase
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
