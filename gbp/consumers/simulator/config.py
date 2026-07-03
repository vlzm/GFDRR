"""EnvironmentConfig — settings for one simulation run."""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .phases import Phase


@dataclasses.dataclass
class EnvironmentConfig:
    """Settings for one run.

    The ordered phases to run, the scenario id, whether to check the run-level
    invariants at the end (on by default, so every plain run is checked), the
    demand scale, and how many periods to step.
    """

    phases: list[Phase]
    scenario_id: str
    validate: bool = True
    demand_scale_factor: float = 1.0
    number_of_periods: int = 10
