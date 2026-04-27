"""Rebalancer — EARLY PROTOTYPE, will be redesigned as a Task.

.. deprecated::
    This package is a standalone PDP solver prototype using OR-Tools.
    It will be replaced by a proper ``Task`` implementation inside
    ``gbp.consumers.simulator.tasks`` that integrates with the
    Environment engine.  Do not extend or build on this code.

.. warning::
    ``DataLoaderRebalancer.load_data`` currently relies on ``df_inventory_ts``
    on the source object, which was removed from ``BikeShareSourceProtocol``
    (it was a mock-only ground-truth fixture and is no longer exposed).
    Calling ``load_data`` will raise ``AttributeError``.  The accompanying
    ``tests/test_rebalancer.py`` was deleted for the same reason.  This
    module stays in the tree only as a reference until the Task rewrite.
"""

from .contracts import RebalancerConfig
from .dataloader import DataLoaderRebalancer
from .pipeline import Rebalancer

__all__ = [
    "DataLoaderRebalancer",
    "Rebalancer",
    "RebalancerConfig",
]
