"""Historical-replay simulator: engine, per-period phases and state.

The public surface is the ``Environment`` (with its ``EnvironmentConfig``),
the three canonical phases that split a period into: dock earlier arrivals ->
form departures (and build the trips) -> dock same-period arrivals, and the
size-then-run entry point ``run_sized_scenario`` (Notations.md §11) that the
runner and the notebook call.
"""

from .config import EnvironmentConfig
from .engine import Environment
from .phases import (
    DockArrivals,
    FormDeparturesPhase,
    Phase,
)
from .scenario import ScenarioRun, canonical_phases, run_sized_scenario
from .sizing import size_state_for_demand
from .state import SimulationState, SimulatorConfigError

__all__ = [
    "Environment",
    "EnvironmentConfig",
    "Phase",
    "DockArrivals",
    "FormDeparturesPhase",
    "ScenarioRun",
    "SimulationState",
    "SimulatorConfigError",
    "canonical_phases",
    "run_sized_scenario",
    "size_state_for_demand",
]
