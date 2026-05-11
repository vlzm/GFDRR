"""NoopTask: a task that produces no dispatches.

Used for testing the engine loop without any dispatch logic.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from gbp.consumers.simulator.task import DISPATCH_COLUMNS

if TYPE_CHECKING:
    from gbp.consumers.simulator.state import PeriodRow, SimulationState
    from gbp.core.model import ResolvedModelData


class NoopTask:
    """Task that return empty dispatches for testing the engine.

    Attributes
    ----------
    name
        Task identifier, always ``"noop"``.
    """

    name: str = "noop"

    def run(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
    ) -> pd.DataFrame:
        """Return an empty dispatches DataFrame.

        Parameters
        ----------
        state
            Current simulation state (unused).
        resolved
            Resolved model data (unused).
        period
            Current period descriptor (unused).

        Returns
        -------
        pd.DataFrame
            Empty DataFrame with ``DISPATCH_COLUMNS`` schema.
        """
        return pd.DataFrame(columns=DISPATCH_COLUMNS)
