"""DispatchPhase: simulation-loop adapter for the dispatch lifecycle.

The phase produces a Task's dispatches, hands them to
:func:`gbp.consumers.simulator.dispatch_lifecycle.run_dispatch_lifecycle`,
and packages the outcome as a :class:`PhaseResult`.  All domain logic
(assignment, validation, application) lives behind the lifecycle's
interface; the phase is just glue between the simulation loop and that
deep module.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from gbp.consumers.simulator.dispatch_lifecycle import run_dispatch_lifecycle
from gbp.consumers.simulator.phases import PhaseResult, Schedule
from gbp.consumers.simulator.task import Task

if TYPE_CHECKING:
    from gbp.consumers.simulator.state import PeriodRow, SimulationState
    from gbp.core.model import ResolvedModelData


class DispatchPhase:
    """Run a Task and feed its output to the dispatch lifecycle.

    Parameters
    ----------
    task
        Domain-specific task that produces dispatches.
    schedule
        Optional execution schedule.  Defaults to every period.

    Attributes
    ----------
    name : str
        Phase name, auto-derived as ``DISPATCH_{task.name}``.
    """

    def __init__(
        self,
        task: Task,
        schedule: Schedule | None = None,
    ) -> None:
        """Initialise with a task and optional schedule."""
        self.name: str = f"DISPATCH_{task.name}"
        self._task = task
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
        """Run the task and apply the resulting dispatches via the lifecycle."""
        dispatches = self._task.run(state, resolved, period)
        outcome = run_dispatch_lifecycle(dispatches, state, resolved, period)

        events: dict[str, pd.DataFrame] = {}
        if not outcome.rejected.empty:
            events["rejected_dispatches"] = outcome.rejected
        if not outcome.flow_events.empty:
            events["flow_events"] = outcome.flow_events
        return PhaseResult(state=outcome.state, events=events)
