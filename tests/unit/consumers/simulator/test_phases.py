"""Tests for Phase protocol, PhaseResult, and Schedule."""
# ruff: noqa: D102

from __future__ import annotations

from datetime import date

from gbp.consumers.simulator.phases import Phase, PhaseResult, Schedule
from gbp.consumers.simulator.state import PeriodRow


def _make_period(period_index: int = 0, period_id: str = "p0") -> PeriodRow:
    """Create a minimal PeriodRow for schedule tests."""
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


class TestSchedule:
    """Schedule predicate behaviour."""

    def test_every_always_true(self) -> None:
        sched = Schedule.every()
        for i in range(10):
            assert sched.should_run(_make_period(period_index=i))

    def test_every_n_fires_at_offset(self) -> None:
        sched = Schedule.every_n(3, offset=1)
        results = [sched.should_run(_make_period(period_index=i)) for i in range(9)]
        assert results == [
            False, True, False,   # 0,1,2
            False, True, False,   # 3,4,5
            False, True, False,   # 6,7,8
        ]

    def test_every_n_default_offset_zero(self) -> None:
        sched = Schedule.every_n(4)
        assert sched.should_run(_make_period(period_index=0))
        assert not sched.should_run(_make_period(period_index=1))
        assert sched.should_run(_make_period(period_index=4))

    def test_custom_predicate(self) -> None:
        sched = Schedule.custom(lambda p: p.period_type == "day")
        assert sched.should_run(_make_period())

        sched_week = Schedule.custom(lambda p: p.period_type == "week")
        assert not sched_week.should_run(_make_period())


class TestPhaseResult:
    """PhaseResult.empty helper."""

    def test_empty_has_no_events(self, resolved_model) -> None:  # type: ignore[no-untyped-def]
        from gbp.consumers.simulator.state import init_state

        state = init_state(resolved_model)
        result = PhaseResult.empty(state)

        assert result.state is state
        assert result.events == {}
        # Convenience accessor returns an empty DataFrame for unknown keys.
        assert result.event("flow_events").empty
        assert result.event("unmet_demand").empty
        assert result.event("rejected_dispatches").empty


class TestPhaseProtocol:
    """A class satisfying the Phase protocol is recognised at runtime."""

    def test_structural_subtyping(self) -> None:
        class _DummyPhase:
            name = "dummy"

            def should_run(self, period: PeriodRow) -> bool:
                return True

            def execute(self, state, resolved, period) -> PhaseResult:  # type: ignore[override]
                return PhaseResult.empty(state)

        assert isinstance(_DummyPhase(), Phase)
