"""Simulation engine: the period-stepping ``Environment`` and its config.

Historical replay (minimal working version). The base scenario re-emits every
historical trip exactly, so that ``simulated_flows_df == historical_flows_df``.
Inventory and in-transit are tracked as real state. The constraints that *change*
outcomes (dock overflow -> redirect, stockout -> lost) are fully implemented in
the phases, but stay dormant in an exact replay (saturated inventory and capacity)
and only become meaningful once demand is pushed above the historical baseline.
"""

import dataclasses

import pandas as pd

from gbp.loaders.dataloader_graph import ResolvedModelData
from gbp.model import empty_flows_journal, empty_in_transit, finalize_flows

from .phases import Phase
from .state import PeriodRow, SimulationState, SimulatorConfigError
from .validation import RunInvariantError, validate_run


@dataclasses.dataclass
class EnvironmentConfig:
    """Settings for one run.

    The ordered phases to run, the seed, the scenario id, and whether to check
    the run-level invariants at the end.
    """

    phases: list[Phase]
    seed: int
    scenario_id: str
    validate: bool = False


def init_state(resolved: ResolvedModelData, first_period: PeriodRow) -> SimulationState:
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

    def __init__(self, resolved: ResolvedModelData, config: EnvironmentConfig) -> None:
        if resolved.potential_trips_df.empty and resolved.initial_inventory_df.empty:
            raise SimulatorConfigError(
                "Environment requires potential_trips_df or initial_inventory_df."
            )
        self._resolved = resolved
        self._config = config
        self._periods: list[PeriodRow] = [
            PeriodRow(int(r.period_id), r.start_timestamp, r.end_timestamp)
            for r in resolved.periods_df.itertuples(index=False)
        ]
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
        return self._period_cursor >= len(self._periods)

    def run(self) -> SimulationState:
        """Step every period to the end, optionally check invariants, return the state."""
        while not self.is_done:
            self.step()
        if self._config.validate:
            violations = validate_run(self._state, self._resolved)
            if violations:
                raise RunInvariantError("run invariants violated:\n" + "\n".join(violations))
        return self._state

    def step(self) -> SimulationState:
        """Run one period: execute each scheduled phase, append its events, advance the clock."""
        period = self._periods[self._period_cursor]
        for phase in self._config.phases:
            if phase.should_run(period):
                result = phase.execute(self._state, self._resolved, period)
                self._state = result.state.append_flows(result.events)

        self._period_cursor += 1
        if not self.is_done:
            self._state = self._state.advance_period(self._periods[self._period_cursor])
        return self._state
