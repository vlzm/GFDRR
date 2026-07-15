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

    ``validate`` on means the engine computes invariants I1-I5 at end of run and
    stores them on ``Environment.violations``; it does not raise, so a caller can
    record a failed run or fail on it. Off skips the check (the sizing run uses
    this, since its own no-loss assert already guards it).
    """

    phases: list[Phase]
    scenario_id: str
    validate: bool = True
    demand_scale_factor: float = 1.0
    number_of_periods: int = 10

    def __post_init__(self) -> None:
        """Reject a config no run can execute, at construction time.

        ``ValueError`` for ``number_of_periods < 1`` and for
        ``demand_scale_factor <= 0``.
        """
        if self.number_of_periods < 1:
            raise ValueError(f"number_of_periods must be >= 1, got {self.number_of_periods}")
        if self.demand_scale_factor <= 0:
            raise ValueError(f"demand_scale_factor must be > 0, got {self.demand_scale_factor}")
