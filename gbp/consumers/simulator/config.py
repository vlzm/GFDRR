"""Environment configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gbp.consumers.simulator.phases import Phase


@dataclass(frozen=True)
class EnvironmentConfig:
    """Configuration for a simulation run.

    Attributes:
        phases: Ordered list of phases to execute each period.
            Execution order = list order.
        seed: Optional random seed for reproducibility of stochastic solvers.
        scenario_id: Identifier for this simulation scenario (used in logs).
    """

    phases: list[Phase] = field(default_factory=list)
    seed: int | None = None
    scenario_id: str = "default"
