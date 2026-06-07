"""Historical-replay simulator: engine, per-period phases and state.

The public surface is the ``Environment`` (with its ``EnvironmentConfig``) and
the four canonical phases that split a period into: dock earlier arrivals ->
form departures -> (sample extra trips) -> dock same-period arrivals.
"""

from .engine import Environment, EnvironmentConfig
from .phases import (
    DockArrivals,
    FormDeparturesPhase,
    FormPotentialTripsPhase,
    Phase,
)
from .state import PhaseResult, Schedule, SimulationState, SimulatorConfigError

__all__ = [
    "Environment",
    "EnvironmentConfig",
    "Phase",
    "DockArrivals",
    "FormDeparturesPhase",
    "FormPotentialTripsPhase",
    "SimulationState",
    "PhaseResult",
    "Schedule",
    "SimulatorConfigError",
]
