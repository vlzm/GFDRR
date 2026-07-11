"""Historical-replay simulator: engine, per-period phases and state.

The public surface is the ``Environment`` (with its ``EnvironmentConfig``),
the three canonical phases that split a period into: dock earlier arrivals ->
form departures (and build the trips) -> dock same-period arrivals, the two
optional overnight-rebalancing phases (Notations.md §14), and the
size-then-run entry point ``run_sized_scenario`` (Notations.md §11) that the
runner and the notebook call. Every entry point takes its scenario data as
``ScenarioInputs`` (Notations.md §11.1) -- the named list of the tables a run
reads; the loader's ``ResolvedModelData`` is one supplier of that contract.
"""

from .config import EnvironmentConfig
from .engine import Environment
from .inputs import ScenarioInputs
from .phases import (
    DockArrivals,
    FormDeparturesPhase,
    Phase,
)
from .rebalancing import (
    ApplyRebalancingPhase,
    PlanRebalancingPhase,
    RebalancingParams,
    rebalancing_phases,
    solve_rebalance_vrp,
)
from .scenario import ScenarioRun, canonical_phases, run_sized_scenario
from .sizing import size_state_for_demand
from .state import SimulationState, SimulatorConfigError

__all__ = [
    "ApplyRebalancingPhase",
    "Environment",
    "EnvironmentConfig",
    "Phase",
    "DockArrivals",
    "FormDeparturesPhase",
    "PlanRebalancingPhase",
    "RebalancingParams",
    "ScenarioInputs",
    "ScenarioRun",
    "SimulationState",
    "SimulatorConfigError",
    "canonical_phases",
    "rebalancing_phases",
    "run_sized_scenario",
    "size_state_for_demand",
    "solve_rebalance_vrp",
]
