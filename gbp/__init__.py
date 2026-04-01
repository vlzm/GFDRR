"""Graph-Based Logistics Platform — universal network flow modelling.

Top-level convenience imports so users can write::

    from gbp import RawModelData, build_model, make_raw_model, Environment

For the full set of schemas and enums, import from subpackages::

    from gbp.core import Facility, FacilityRole, AttributeRegistry
    from gbp.consumers.simulator import DemandPhase, ArrivalsPhase
    from gbp.io import save_raw_parquet, load_raw_parquet
"""

# ── Data model ───────────────────────────────────────────────────────
from gbp.core.model import RawModelData, ResolvedModelData

# ── Build pipeline ───────────────────────────────────────────────────
from gbp.build.pipeline import build_model

# ── Quick-start factory ──────────────────────────────────────────────
from gbp.core.factory import make_raw_model

# ── Simulation engine ────────────────────────────────────────────────
from gbp.consumers.simulator.engine import Environment
from gbp.consumers.simulator.config import EnvironmentConfig

# ── Key enums ────────────────────────────────────────────────────────
from gbp.core.enums import (
    AttributeKind,
    FacilityRole,
    FacilityType,
    ModalType,
    OperationType,
    PeriodType,
)

# ── Attribute system ─────────────────────────────────────────────────
from gbp.core.attributes.registry import AttributeRegistry

__all__ = [
    "AttributeKind",
    "AttributeRegistry",
    "build_model",
    "Environment",
    "EnvironmentConfig",
    "FacilityRole",
    "FacilityType",
    "make_raw_model",
    "ModalType",
    "OperationType",
    "PeriodType",
    "RawModelData",
    "ResolvedModelData",
]
