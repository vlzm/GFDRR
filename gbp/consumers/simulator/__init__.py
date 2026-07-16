"""Historical-replay simulator: engine, per-period phases and state."""

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
from .scenario import (
    ScenarioRun,
    canonical_phases,
    run_sized_scenario,
    scaled_demand_inputs,
)
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
    "scaled_demand_inputs",
    "size_state_for_demand",
    "solve_rebalance_vrp",
]
