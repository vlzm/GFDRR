"""AttributeSpec: unified description of a numeric (or additional) attribute."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from gbp.core.enums import AttributeKind

VALID_AGGREGATIONS: frozenset[str] = frozenset({"mean", "sum", "min", "max", "first", "last"})

_ENTITY_GRAINS: dict[str, tuple[str, ...]] = {
    "facility": ("facility_id",),
    "edge": ("source_id", "target_id", "modal_type"),
    # Aligns with ``ResourceCost.resource_category`` and fleet FK naming.
    "resource": ("resource_category",),
}


@dataclass(frozen=True)
class AttributeSpec:
    """Describes one attribute column merged into a spine table.

    ``grain`` uses ``date`` for time-varying raw tables; ``resolved_grain`` uses
    ``period_id`` after the build pipeline time-resolution step.
    """

    name: str
    kind: AttributeKind
    entity_type: str
    grain: tuple[str, ...]
    resolved_grain: tuple[str, ...]
    value_column: str
    source_table: str
    unit: str | None = None
    aggregation: str = "mean"
    nullable: bool = False
    eav_filter: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        """Validate entity grains, aggregation, and time-varying consistency."""
        if self.aggregation not in VALID_AGGREGATIONS:
            raise ValueError(
                f"Attribute {self.name!r}: aggregation {self.aggregation!r} is not valid; "
                f"choose from {sorted(VALID_AGGREGATIONS)}"
            )
        if self.entity_type not in _ENTITY_GRAINS:
            raise ValueError(
                f"entity_type must be one of {sorted(_ENTITY_GRAINS)}, got {self.entity_type!r}"
            )
        eg = set(self.entity_grain)
        if not eg.issubset(self.grain):
            raise ValueError(
                f"Attribute {self.name!r}: entity_grain {tuple(sorted(eg))} "
                f"must be a subset of grain {self.grain}"
            )
        if not eg.issubset(self.resolved_grain):
            raise ValueError(
                f"Attribute {self.name!r}: entity_grain {tuple(sorted(eg))} "
                f"must be a subset of resolved_grain {self.resolved_grain}"
            )
        if "date" in self.grain and "period_id" not in self.resolved_grain:
            raise ValueError(
                f"Attribute {self.name!r}: time-varying grain contains 'date' but "
                f"resolved_grain has no 'period_id'"
            )
        if "date" in self.resolved_grain:
            raise ValueError(f"Attribute {self.name!r}: resolved_grain must not contain 'date'")

    @property
    def entity_grain(self) -> tuple[str, ...]:
        """Identity columns for the entity (facility, edge, or resource category)."""
        return _ENTITY_GRAINS[self.entity_type]

    @property
    def time_varying(self) -> bool:
        """True when raw grain includes calendar ``date``."""
        return "date" in self.grain

    def resolved_merge_grain(self) -> tuple[str, ...]:
        """Grain columns used when merging resolved attribute tables."""
        return self.resolved_grain
