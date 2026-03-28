"""Phase protocol, PhaseResult, and Schedule for the simulation engine.

A **Phase** is one logical operation within a period (e.g. DEMAND, ARRIVALS,
DISPATCH).  Phases implement a ``Protocol`` so that both built-in and custom
phases share the same contract.

``Schedule`` controls *when* a phase fires (every period, every Nth, custom
predicate).  ``PhaseResult`` bundles the updated state with event DataFrames
for logging.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import pandas as pd

if TYPE_CHECKING:
    from gbp.consumers.simulator.state import PeriodRow, SimulationState
    from gbp.core.model import ResolvedModelData


# -- PhaseResult ---------------------------------------------------------------


@dataclass
class PhaseResult:
    """Output of a single phase execution.

    Attributes:
        state: Updated simulation state after the phase.
        flow_events: Commodity movements produced by this phase (-> flow_log).
        unmet_demand: Demand that could not be fulfilled (-> unmet_demand_log).
        rejected_dispatches: Dispatches rejected during validation
            (-> rejected_dispatches_log).
    """

    state: SimulationState
    flow_events: pd.DataFrame = field(default_factory=lambda: pd.DataFrame())
    unmet_demand: pd.DataFrame = field(default_factory=lambda: pd.DataFrame())
    rejected_dispatches: pd.DataFrame = field(default_factory=lambda: pd.DataFrame())

    @classmethod
    def empty(cls, state: SimulationState) -> PhaseResult:
        """Create a no-op result that passes the state through unchanged."""
        return cls(state=state)


# -- Schedule ------------------------------------------------------------------


@dataclass(frozen=True)
class Schedule:
    """Determines in which periods a phase should run.

    Uses a callable predicate for maximum flexibility.  Convenience
    constructors cover common patterns.

    Attributes:
        predicate: Function ``(PeriodRow) -> bool``.
    """

    predicate: Callable[[PeriodRow], bool]

    def should_run(self, period: PeriodRow) -> bool:
        """Return whether the phase should execute in *period*."""
        return self.predicate(period)

    # -- Convenience constructors ----------------------------------------------

    @staticmethod
    def every() -> Schedule:
        """Run every period."""
        return Schedule(predicate=lambda _p: True)

    @staticmethod
    def every_n(n: int, offset: int = 0) -> Schedule:
        """Run every *n*-th period, starting from *offset*.

        Args:
            n: Interval between executions.
            offset: First period index to execute on (modulo *n*).
        """
        return Schedule(predicate=lambda p: p.period_index % n == offset)

    @staticmethod
    def custom(predicate: Callable[[PeriodRow], bool]) -> Schedule:
        """Run when *predicate* returns ``True``."""
        return Schedule(predicate=predicate)


# -- Phase Protocol ------------------------------------------------------------


@runtime_checkable
class Phase(Protocol):
    """Contract for a logical operation within a simulation period."""

    name: str

    def should_run(self, period: PeriodRow) -> bool:
        """Whether this phase should execute in the given period."""
        ...

    def execute(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
    ) -> PhaseResult:
        """Execute the phase logic and return updated state + events."""
        ...
