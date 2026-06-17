"""EnvironmentConfig — settings for one simulation run."""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .phases import Phase


@dataclasses.dataclass
class EnvironmentConfig:
    """Settings for one run.

    The ordered phases to run, the seed, the scenario id, and whether to check
    the run-level invariants at the end.
    """

    phases: list[Phase]
    seed: int
    scenario_id: str
    validate: bool = False
    demand_scale_factor: float = 1.0
    number_of_periods: int = 10
