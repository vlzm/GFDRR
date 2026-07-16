"""EnvironmentConfig — settings for one simulation run."""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .phases import Phase


@dataclasses.dataclass
class EnvironmentConfig:
    """Settings for one run."""

    phases: list[Phase]
    scenario_id: str
    validate: bool = True
    number_of_periods: int = 10

    def __post_init__(self) -> None:
        """Reject a config no run can execute, at construction time."""
        if self.number_of_periods < 1:
            raise ValueError(f"number_of_periods must be >= 1, got {self.number_of_periods}")
