"""Graph-Based Logistics Platform — vertical bike-sharing simulation.

Top-level convenience imports so users can write::

    from gbp import RawModelData, build_model, Environment
"""

# ── Data model ───────────────────────────────────────────────────────
# ── Build pipeline ───────────────────────────────────────────────────
from gbp.build.pipeline import build_model
from gbp.consumers.simulator.config import EnvironmentConfig

# ── Simulation engine ────────────────────────────────────────────────
from gbp.consumers.simulator.engine import Environment

# ── Attribute system ─────────────────────────────────────────────────
from gbp.core.attributes.registry import AttributeRegistry

# ── Key enums ────────────────────────────────────────────────────────
from gbp.core.enums import (
    AttributeKind,
    FacilityRole,
    FacilityType,
    ModalType,
    OperationType,
    PeriodType,
)
from gbp.core.model import RawModelData, ResolvedModelData

__all__ = [
    "AttributeKind",
    "AttributeRegistry",
    "build_model",
    "Environment",
    "EnvironmentConfig",
    "FacilityRole",
    "FacilityType",
    "ModalType",
    "OperationType",
    "PeriodType",
    "RawModelData",
    "ResolvedModelData",
]
