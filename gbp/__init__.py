"""Citi Bike Simulation Platform — vertical bike-sharing simulation.

Top-level convenience imports so users can write::

    from gbp import RawModelData, ResolvedModelData, Environment
"""

from gbp.consumers.simulator import Environment, EnvironmentConfig
from gbp.loaders import RawModelData, ResolvedModelData, attach_simulation

__all__ = [
    "RawModelData",
    "ResolvedModelData",
    "attach_simulation",
    "Environment",
    "EnvironmentConfig",
]
