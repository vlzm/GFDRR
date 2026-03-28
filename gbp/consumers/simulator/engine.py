"""Environment: step-by-step simulation engine.

The ``Environment`` class orchestrates the simulation loop.  It iterates
over periods from ``ResolvedModelData``, executing configured phases in order,
and accumulates results in a ``SimulationLog``.

Three levels of granularity:
- ``run()`` — full simulation through all periods.
- ``step()`` — one period (all phases).
- ``step_phase(name)`` — one named phase within the current period.
"""

from __future__ import annotations

from gbp.consumers.simulator.config import EnvironmentConfig
from gbp.consumers.simulator.log import SimulationLog
from gbp.consumers.simulator.state import PeriodRow, SimulationState, init_state
from gbp.core.model import ResolvedModelData


class Environment:
    """Step-by-step simulation engine.

    Args:
        resolved: Fully resolved model from ``build_model()``.
        config: Simulation configuration (phases, seed, scenario_id).
    """

    def __init__(
        self,
        resolved: ResolvedModelData,
        config: EnvironmentConfig,
    ) -> None:
        """Initialise the environment from resolved data and config."""
        self._resolved = resolved
        self._config = config
        self._state = init_state(resolved)
        self._log = SimulationLog()
        self._periods: list[PeriodRow] = [
            PeriodRow(*row) for row in resolved.periods.itertuples()
        ]
        self._period_cursor: int = 0

    # -- Properties ------------------------------------------------------------

    @property
    def state(self) -> SimulationState:
        """Current simulation state (read-only access)."""
        return self._state

    @property
    def log(self) -> SimulationLog:
        """Accumulated simulation log."""
        return self._log

    @property
    def is_done(self) -> bool:
        """Whether all periods have been processed."""
        return self._period_cursor >= len(self._periods)

    # -- Execution -------------------------------------------------------------

    def run(self) -> SimulationLog:
        """Run the full simulation through all remaining periods.

        Returns:
            The accumulated ``SimulationLog``.
        """
        while not self.is_done:
            self.step()
        return self._log

    def step(self) -> SimulationState:
        """Execute all phases for the current period and advance.

        Returns:
            The simulation state after this period.

        Raises:
            StopIteration: If the simulation is already done.
        """
        if self.is_done:
            raise StopIteration("All periods have been processed.")

        period = self._periods[self._period_cursor]

        for phase in self._config.phases:
            if phase.should_run(period):
                result = phase.execute(self._state, self._resolved, period)
                self._state = result.state
                self._log.record_events(result, phase.name, period)

        self._log.record_period(self._state, period)

        # Advance to next period
        self._period_cursor += 1
        if not self.is_done:
            next_period = self._periods[self._period_cursor]
            self._state = self._state.advance_period(
                next_period_index=next_period.period_index,
                next_period_id=next_period.period_id,
            )

        return self._state

    def step_phase(self, phase_name: str) -> SimulationState:
        """Execute a single named phase in the current period.

        Useful for debugging and testing individual phase behaviour.

        Args:
            phase_name: The ``name`` attribute of the phase to execute.

        Returns:
            The simulation state after executing the phase.

        Raises:
            StopIteration: If the simulation is already done.
            ValueError: If no phase with *phase_name* is found.
        """
        if self.is_done:
            raise StopIteration("All periods have been processed.")

        period = self._periods[self._period_cursor]
        phase = next(
            (p for p in self._config.phases if p.name == phase_name),
            None,
        )
        if phase is None:
            msg = (
                f"No phase named {phase_name!r}. "
                f"Available: {[p.name for p in self._config.phases]}"
            )
            raise ValueError(msg)

        if phase.should_run(period):
            result = phase.execute(self._state, self._resolved, period)
            self._state = result.state
            self._log.record_events(result, phase.name, period)

        return self._state
