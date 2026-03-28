"""Tests for DispatchPhase."""
# ruff: noqa: D102

from __future__ import annotations

from datetime import date

import pandas as pd

from gbp.consumers.simulator.dispatch_phase import DispatchPhase
from gbp.consumers.simulator.log import RejectReason
from gbp.consumers.simulator.phases import Phase, Schedule
from gbp.consumers.simulator.state import PeriodRow, init_state
from gbp.consumers.simulator.task import DISPATCH_COLUMNS
from gbp.core.enums import ResourceStatus
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


class _EmptyTask:
    """Task that returns empty dispatches."""

    name = "empty"

    def run(self, state, resolved, period) -> pd.DataFrame:  # type: ignore[override]
        return pd.DataFrame(columns=DISPATCH_COLUMNS)


class _SingleDispatchTask:
    """Task that always dispatches 5 working_bike from d1 to s1."""

    name = "single"

    def run(self, state, resolved, period) -> pd.DataFrame:  # type: ignore[override]
        return pd.DataFrame(
            {
                "source_id": ["d1"],
                "target_id": ["s1"],
                "commodity_category": ["working_bike"],
                "quantity": [5.0],
                "resource_id": [None],
                "modal_type": ["road"],
                "arrival_period": [period.period_index + 1],
            }
        )


class TestDispatchPhaseContract:
    """DispatchPhase satisfies the Phase protocol."""

    def test_is_phase(self) -> None:
        phase = DispatchPhase(task=_EmptyTask())
        assert isinstance(phase, Phase)

    def test_name_derived_from_task(self) -> None:
        phase = DispatchPhase(task=_EmptyTask())
        assert phase.name == "DISPATCH_empty"

    def test_should_run_delegates_to_schedule(self) -> None:
        phase = DispatchPhase(task=_EmptyTask(), schedule=Schedule.every_n(3))
        assert phase.should_run(_make_period(period_index=0))
        assert not phase.should_run(_make_period(period_index=1))
        assert phase.should_run(_make_period(period_index=3))

    def test_empty_dispatches_return_empty_result(
        self, resolved_model: ResolvedModelData
    ) -> None:
        state = init_state(resolved_model)
        phase = DispatchPhase(task=_EmptyTask())

        result = phase.execute(state, resolved_model, _make_period())

        assert result.state is state
        assert result.flow_events.empty


class TestDispatchPhaseValidation:
    """DispatchPhase validation and application."""

    def test_valid_dispatch_applied(
        self, resolved_model: ResolvedModelData
    ) -> None:
        state = init_state(resolved_model)
        phase = DispatchPhase(task=_SingleDispatchTask())

        result = phase.execute(state, resolved_model, _make_period())

        # Inventory at d1 should decrease by 5
        new_inv = result.state.inventory
        d1_qty = new_inv.loc[
            new_inv["facility_id"] == "d1", "quantity"
        ].iloc[0]
        assert d1_qty == 50.0 - 5.0

        # in_transit should have 1 shipment
        assert len(result.state.in_transit) == 1

        # Flow events should have 1 entry
        assert len(result.flow_events) == 1

    def test_insufficient_inventory_rejected(
        self, resolved_model: ResolvedModelData
    ) -> None:
        state = init_state(resolved_model)
        # Set d1 inventory to 0
        inv = state.inventory.copy()
        inv.loc[inv["facility_id"] == "d1", "quantity"] = 0.0
        state = state.with_inventory(inv)

        phase = DispatchPhase(task=_SingleDispatchTask())
        result = phase.execute(state, resolved_model, _make_period())

        assert not result.rejected_dispatches.empty
        assert result.state.in_transit.empty
        assert (
            result.rejected_dispatches.iloc[0]["reason"]
            == RejectReason.INSUFFICIENT_INVENTORY.value
        )

    def test_auto_assign_resource(
        self, resolved_model: ResolvedModelData
    ) -> None:
        """Dispatch with resource_id=None auto-assigns an available truck."""
        state = init_state(resolved_model)
        phase = DispatchPhase(task=_SingleDispatchTask())

        result = phase.execute(state, resolved_model, _make_period())

        # Resource should now be IN_TRANSIT
        res = result.state.resources
        assigned = res[res["status"] == ResourceStatus.IN_TRANSIT.value]
        assert len(assigned) == 1
        assert assigned.iloc[0]["current_facility_id"] == "s1"

    def test_resource_status_updated(
        self, resolved_model: ResolvedModelData
    ) -> None:
        """After dispatch, resource gets IN_TRANSIT and correct available_at."""
        state = init_state(resolved_model)
        phase = DispatchPhase(task=_SingleDispatchTask())

        result = phase.execute(state, resolved_model, _make_period(0, "p0"))

        res = result.state.resources
        dispatched = res[res["status"] == ResourceStatus.IN_TRANSIT.value]
        assert len(dispatched) == 1
        assert dispatched.iloc[0]["available_at_period"] == 1  # arrival = 0 + 1
