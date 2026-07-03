"""Historical-replay simulator: engine, per-period phases and state.

The public surface is the ``Environment`` (with its ``EnvironmentConfig``) and
the three canonical phases that split a period into: dock earlier arrivals ->
form departures (and build the trips) -> dock same-period arrivals.
"""

from .config import EnvironmentConfig
from .engine import Environment
from .phases import (
    DockArrivals,
    FormDeparturesPhase,
    Phase,
)
from .sizing import size_state_for_demand
from .state import Schedule, SimulationState, SimulatorConfigError

__all__ = [
    "Environment",
    "EnvironmentConfig",
    "Phase",
    "DockArrivals",
    "FormDeparturesPhase",
    "SimulationState",
    "Schedule",
    "SimulatorConfigError",
    "size_state_for_demand",
]
