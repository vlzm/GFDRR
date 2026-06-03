@dataclasses.dataclass
class EnvironmentConfig:
    phases: list
    seed: int
    scenario_id: str

# -- Environment ------------------------------------------------------------
class Environment:
    def __init__(self, resolved: ResolvedModelData, config: EnvironmentConfig) -> None:
        if resolved.potential_trips.empty and resolved.inventory_initial.empty:
            raise SimulatorConfigError(
                "Environment requires potential_trips or inventory_initial."
            )
        self._resolved = resolved
        self._config = config
        self._state = init_state(resolved)
        self._log = SimulationLog()
        self._periods: list[PeriodRow] = [
            PeriodRow(int(r.period_id), r.start_timestamp, r.end_timestamp)
            for r in resolved.periods.itertuples(index=False)
        ]
        self._period_cursor: int = 0

    @property
    def state(self) -> SimulationState:
        return self._state

    @property
    def log(self) -> SimulationLog:
        return self._log

    @property
    def is_done(self) -> bool:
        return self._period_cursor >= len(self._periods)

    def run(self) -> SimulationLog:
        while not self.is_done:
            self.step()
        return self._log

    def step(self) -> SimulationState:
        period = self._periods[self._period_cursor]

        for phase in self._config.phases:
            if phase.should_run(period):
                result = phase.execute(self._state, self._resolved, period)
                self._state = result.state
                self._log.record_events(result, phase.name, period)

        self._log.record_period(self._state, period)

        self._period_cursor += 1
        if not self.is_done:
            self._state = self._state.advance_period(
                self._periods[self._period_cursor].period_id
            )
        return self._state
    
def init_state(resolved: ResolvedModelData) -> SimulationState:
    return SimulationState(
        period_id=int(resolved.periods["period_id"].iloc[0]),
        inventory=resolved.inventory_initial.copy(),
        in_transit=empty_in_transit(),
        resources=pd.DataFrame(),
    )