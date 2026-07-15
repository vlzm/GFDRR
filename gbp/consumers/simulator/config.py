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
    invariants at the end (on by default, so every plain run is checked), and
    how many periods to step. The demand scale is not here: it is bound into the
    demand table once at the run boundary (``scaled_demand_inputs``), so the
    engine and every phase read the demand the run actually faces instead of
    rescaling it.

    ``validate`` on means the engine computes invariants I1-I5 at end of run and
    stores them on ``Environment.violations``; it does not raise, so a caller can
    record a failed run or fail on it. Off skips the check (the sizing run uses
    this, since its own no-loss assert already guards it).
    """

    phases: list[Phase]
    scenario_id: str
    validate: bool = True
    number_of_periods: int = 10

    def __post_init__(self) -> None:
        """Reject a config no run can execute, at construction time.

        ``ValueError`` for ``number_of_periods < 1``.
        """
        if self.number_of_periods < 1:
            raise ValueError(f"number_of_periods must be >= 1, got {self.number_of_periods}")
