"""Tests for DispatchPhase — the simulation-loop adapter.

Behavior tests for assignment / validation / application live in
``test_dispatch_lifecycle.py``.  This file covers the Phase contract and the
Task -> lifecycle wiring.
"""
# ruff: noqa: D102

from __future__ import annotations

from datetime import date

import pandas as pd

from gbp.consumers.simulator.dispatch_phase import DispatchPhase
from gbp.consumers.simulator.phases import Phase, Schedule
from gbp.consumers.simulator.state import PeriodRow, init_state
from gbp.consumers.simulator.task import DISPATCH_COLUMNS
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
    """DispatchPhase satisfies the Phase protocol and delegates scheduling."""

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


class TestDispatchPhaseWiring:
    """The phase passes Task output through the lifecycle and packages events."""

    def test_empty_dispatches_return_empty_result(
        self, resolved_model: ResolvedModelData,
    ) -> None:
        state = init_state(resolved_model)
        phase = DispatchPhase(task=_EmptyTask())

        result = phase.execute(state, resolved_model, _make_period())

        assert result.state is state
        assert result.events == {}

    def test_applied_dispatch_surfaces_flow_event(
        self, resolved_model: ResolvedModelData,
    ) -> None:
        """End-to-end smoke check: Task -> lifecycle -> PhaseResult.events."""
        state = init_state(resolved_model)
        phase = DispatchPhase(task=_SingleDispatchTask())

        result = phase.execute(state, resolved_model, _make_period())

        assert "flow_events" in result.events
        assert len(result.event("flow_events")) == 1
        # The lifecycle did its job: state advanced.
        assert len(result.state.in_transit) == 1
