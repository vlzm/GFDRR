"""Task protocol and dispatch column constants.

A **Task** is a domain-specific decision-maker that produces dispatches
(commodity movements via resources).  Tasks follow the pattern
``prepare -> solve -> postprocess`` internally, but expose a single
``run()`` method returning a dispatches DataFrame.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

import pandas as pd

if TYPE_CHECKING:
    from gbp.consumers.simulator.state import PeriodRow, SimulationState
    from gbp.core.model import ResolvedModelData


DISPATCH_COLUMNS: list[str] = [
    "source_id",
    "target_id",
    "commodity_category",
    "quantity",
    "resource_id",
    "modal_type",
    "arrival_period",
]
"""Standard columns for the dispatches DataFrame returned by ``Task.run()``."""


class Task(Protocol):
    """Domain-specific task: prepare -> solve -> postprocess.

    Returns dispatches as a DataFrame.  ``resource_id`` may be null —
    the ``DispatchPhase`` will auto-assign available resources.
    ``arrival_period`` should be computed by the Task from lead times
    in resolved data.

    Attributes
    ----------
    name : str
        Unique task name used in phase naming and logging.
    """

    name: str

    def run(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
    ) -> pd.DataFrame:
        """Execute the task and return a dispatches DataFrame.

        Parameters
        ----------
        state
            Current simulation state.
        resolved
            Resolved model data.
        period
            Current period descriptor.

        Returns
        -------
        pd.DataFrame
            Dispatches with columns: source_id, target_id,
            commodity_category, quantity, resource_id (nullable),
            modal_type (nullable), arrival_period (int, computed from
            lead times).
        """
        ...
