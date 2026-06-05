"""Historical-replay simulator: engine, per-period phases and state.

The public surface is the ``Environment`` (with its ``EnvironmentConfig``) and
the six canonical phases that split a period into: dock earlier arrivals ->
(redirect) -> form departures -> (sample extra trips) -> dock same-period
arrivals -> (redirect).
"""

from .engine import Environment, EnvironmentConfig
from .phases import (
    ArrivalsPhase,
    ArrivalsPreviousPhase,
    FormDeparturesPhase,
    FormPotentialTripsPhase,
    OverflowRedirectPhase,
    OverflowRedirectPreviousPhase,
    Phase,
)
from .state import PhaseResult, Schedule, SimulationState, SimulatorConfigError

__all__ = [
    "Environment",
    "EnvironmentConfig",
    "Phase",
    "ArrivalsPreviousPhase",
    "OverflowRedirectPreviousPhase",
    "FormDeparturesPhase",
    "FormPotentialTripsPhase",
    "ArrivalsPhase",
    "OverflowRedirectPhase",
    "SimulationState",
    "PhaseResult",
    "Schedule",
    "SimulatorConfigError",
]
