"""Simulation engine: the period-stepping ``Environment`` and its config.

Historical replay (minimal working version). The base scenario re-emits every
historical trip exactly, so that ``simulated_flows_df == historical_flows_df``.
Inventory and in-transit are tracked as real state. The constraints that *change*
outcomes (dock overflow -> redirect, stockout -> lost) are fully implemented in
the phases, but stay dormant in an exact replay (saturated stock and capacity)
and only become meaningful once demand is pushed above the historical baseline.
"""

import dataclasses

import pandas as pd

from gbp.loaders.dataloader_graph import ResolvedModelData

from .state import (
    PeriodRow,
    SimulationState,
    SimulatorConfigError,
    empty_flows_journal,
    empty_in_transit,
    finalize_flows,
)


@dataclasses.dataclass
class EnvironmentConfig:
    phases: list
    seed: int
    scenario_id: str


def init_state(resolved: ResolvedModelData, first_period: PeriodRow) -> SimulationState:
    return SimulationState(
        state_period_id_obj=first_period,
        state_inventory_df=resolved.initial_inventory_df.copy(),
        state_flows_df=empty_flows_journal(),
        state_resources_df=pd.DataFrame(),
        in_transit=empty_in_transit(),
    )


class Environment:
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
        return self._state

    @property
    def simulated_flows_df(self) -> pd.DataFrame:
        """Finalized flow journal of the run (identical in shape to history)."""
        return finalize_flows(self._state.state_flows_df)

    @property
    def is_done(self) -> bool:
        return self._period_cursor >= len(self._periods)

    def run(self) -> SimulationState:
        while not self.is_done:
            self.step()
        return self._state

    def step(self) -> SimulationState:
        period = self._periods[self._period_cursor]

        for phase in self._config.phases:
            if phase.should_run(period):
                result = phase.execute(self._state, self._resolved, period)
                self._state = result.state.append_flows(result.events)

        self._period_cursor += 1
        if not self.is_done:
            self._state = self._state.advance_period(self._periods[self._period_cursor])
        return self._state
