"""Simulation engine: the period-stepping ``Environment`` and its config.

Historical replay (minimal working version). The base scenario re-emits every
historical trip exactly, so that ``simulated_flows_df == historical_flows_df``.
Inventory and in-transit are tracked as real state. The constraints that *change*
outcomes (dock overflow -> redirect, stockout -> lost) are fully implemented in
the phases, but do nothing in an exact replay (saturated inventory and capacity)
and only become meaningful once demand is pushed above the historical baseline.
"""

import pandas as pd

from gbp.model import empty_flows_journal, empty_in_transit, finalize_flows

from .config import EnvironmentConfig
from .inputs import ScenarioInputs
from .state import PeriodRow, SimulationState, SimulatorConfigError
from .validation import RunInvariantError, validate_run


def init_state(resolved: ScenarioInputs, first_period: PeriodRow) -> SimulationState:
    """Build the starting state for a run: initial inventory and an empty journal."""
    return SimulationState(
        state_period_id_obj=first_period,
        state_inventory_df=resolved.initial_inventory_df.copy(),
        state_flows_df=empty_flows_journal(),
        state_resources_df=pd.DataFrame(),
        in_transit=empty_in_transit(),
    )


class Environment:
    """Steps a run period by period.

    Each period runs the scheduled phases, appends their events to the journal,
    and advances the clock.
    """

    def __init__(self, resolved: ScenarioInputs, config: EnvironmentConfig) -> None:
        if resolved.historical_demand_df.empty and resolved.initial_inventory_df.empty:
            raise SimulatorConfigError(
                "Environment requires historical_demand_df or initial_inventory_df."
            )
        self._resolved = resolved
        self._config = config
        self._periods: list[PeriodRow] = [
            PeriodRow(int(r.period_id), r.start_timestamp, r.end_timestamp)
            for r in resolved.periods_df.itertuples(index=False)
        ]
        # Asking for more periods than the grid holds would silently run fewer
        # (and the flow-closure invariant would measure a truncated horizon), so
        # refuse it up front instead.
        if config.number_of_periods > len(self._periods):
            raise SimulatorConfigError(
                f"number_of_periods={config.number_of_periods} exceeds the period grid "
                f"({len(self._periods)} periods)"
            )
        # Phases execute in list order and the state's counter hands out
        # step_id in that order, while the declared phase_rank sorts the steps.
        # A list out of rank order would write a journal whose step_id and
        # phase_rank orders disagree.
        ranks = [phase.phase_rank for phase in config.phases]
        if ranks != sorted(ranks):
            raise SimulatorConfigError(f"phases must be ordered by phase_rank, got {ranks}")
        self._period_cursor: int = 0
        self._state = init_state(resolved, self._periods[0])

    @property
    def state(self) -> SimulationState:
        """The current simulation state."""
        return self._state

    @property
    def simulated_flows_df(self) -> pd.DataFrame:
        """Finalized flow journal of the run (identical in shape to history)."""
        return finalize_flows(self._state.state_flows_df)

    @property
    def is_done(self) -> bool:
        """True once every period has been stepped."""
        return self._period_cursor >= len(self._periods[: self._config.number_of_periods])

    def run(self) -> SimulationState:
        """Step every period to the end, check the run invariants, return the state.

        The invariant check (I1-I5) runs by default (``EnvironmentConfig.validate``);
        it costs one extra ``finalize_flows`` plus one ``inventory_at_moments``
        pass over the finished journal.
        """
        while not self.is_done:
            self.step()
        if self._config.validate:
            violations = validate_run(
                self._state,
                self._resolved,
                self._config.demand_scale_factor,
                self._config.number_of_periods,
            )
            if violations:
                raise RunInvariantError("run invariants violated:\n" + "\n".join(violations))
        return self._state

    def step(self) -> SimulationState:
        """Run one period: execute each phase in order, advance the clock.

        Each phase writes its own events to the journal through
        :meth:`SimulationState.apply_step_events` and returns the next state.
        """
        period = self._periods[self._period_cursor]
        for phase in self._config.phases:
            self._state = phase.execute(self._state, self._resolved, period, self._config)

        self._period_cursor += 1
        if not self.is_done:
            self._state = self._state.advance_period(self._periods[self._period_cursor])
        return self._state
