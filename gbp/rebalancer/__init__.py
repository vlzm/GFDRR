"""Rebalancer — EARLY PROTOTYPE, will be redesigned as a Task.

.. deprecated::
    This package is a standalone PDP solver prototype using OR-Tools.
    It will be replaced by a proper ``Task`` implementation inside
    ``gbp.consumers.simulator.tasks`` that integrates with the
    Environment engine.  Do not extend or build on this code.
"""

from .contracts import RebalancerConfig
from .dataloader import DataLoaderRebalancer
from .pipeline import Rebalancer

__all__ = [
    "DataLoaderRebalancer",
    "Rebalancer",
    "RebalancerConfig",
]
