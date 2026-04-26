"""Simulation engine (Environment) — step-by-step digital twin.

Public API::

    from gbp.consumers.simulator import Environment, EnvironmentConfig
    env = Environment(resolved, config)
    log = env.run()
"""

from gbp.consumers.simulator.built_in_phases import (
    ArrivalsPhase,
    DemandPhase,
    OrganicFlowPhase,
)
from gbp.consumers.simulator.config import EnvironmentConfig
from gbp.consumers.simulator.dispatch_phase import DispatchPhase
from gbp.consumers.simulator.engine import Environment
from gbp.consumers.simulator.log import RejectReason, SimulationLog
from gbp.consumers.simulator.phases import Phase, PhaseResult, Schedule
from gbp.consumers.simulator.state import PeriodRow, SimulationState, init_state
from gbp.consumers.simulator.task import DISPATCH_COLUMNS, Task

__all__ = [
    "ArrivalsPhase",
    "DemandPhase",
    "DISPATCH_COLUMNS",
    "DispatchPhase",
    "Environment",
    "EnvironmentConfig",
    "init_state",
    "OrganicFlowPhase",
    "Phase",
    "PhaseResult",
    "PeriodRow",
    "RejectReason",
    "Schedule",
    "SimulationLog",
    "SimulationState",
    "Task",
]
