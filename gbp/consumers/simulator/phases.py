"""Phase protocol, PhaseResult, and Schedule for the simulation engine.

A **Phase** is one logical operation within a period (e.g. DEMAND, ARRIVALS,
DISPATCH).  Phases implement a ``Protocol`` so that both built-in and custom
phases share the same contract.

``Schedule`` controls *when* a phase fires (every period, every Nth, custom
predicate).  ``PhaseResult`` bundles the updated state with a dict of named
event DataFrames.  Each key routes to a specific log table via the registry
in :mod:`gbp.consumers.simulator.log`.
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
        events: Mapping of event-table short name to the rows produced by this
            phase.  Each key must match a registered log table (see
            ``gbp.consumers.simulator.log.LOG_TABLES``).  Empty or missing
            entries are skipped by ``SimulationLog.record_events``.

    Common keys:
        - ``"flow_events"``        — commodity movements (-> simulation_flow_log)
        - ``"unmet_demand"``       — demand not fulfilled by inventory
        - ``"rejected_dispatches"`` — dispatches rejected during validation
        - ``"latent_demand"``      — origin/destination marginals
        - ``"lost_demand"``        — gap between latent and realised departures
        - ``"dock_blocking"``      — arrivals refused over storage capacity
    """

    state: SimulationState
    events: dict[str, pd.DataFrame] = field(default_factory=dict)

    @classmethod
    def empty(cls, state: SimulationState) -> PhaseResult:
        """Create a no-op result that passes the state through unchanged."""
        return cls(state=state)

    def event(self, name: str) -> pd.DataFrame:
        """Return the event DataFrame for *name*, or an empty DataFrame.

        Convenience for callers and tests that want a single-line check.
        Does not enforce schema; the log layer validates against the registry.
        """
        df = self.events.get(name)
        return df if df is not None else pd.DataFrame()


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
