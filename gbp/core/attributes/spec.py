"""Unified description of a numeric (or additional) attribute."""

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
    """Describe one attribute column merged into a spine table.

    ``grain`` uses ``date`` for time-varying raw tables; ``resolved_grain``
    uses ``period_id`` after the build pipeline time-resolution step.

    Parameters
    ----------
    name
        Unique attribute identifier.
    kind
        Semantic kind (cost, capacity, rate, revenue, additional).
    entity_type
        One of ``"facility"``, ``"edge"``, ``"resource"``.
    grain
        Raw grain columns including ``"date"`` for time-varying attributes.
    resolved_grain
        Grain columns after time resolution (``"date"`` replaced by
        ``"period_id"``).
    value_column
        Column in the source data that holds the attribute value.
    source_table
        Name of the raw data table or registry key.
    unit
        Physical unit string. Default is ``None``.
    aggregation
        Aggregation function name. Default is ``"mean"``.
    nullable
        Whether missing values are allowed. Default is ``False``.
    eav_filter
        Column-value pairs used to filter EAV-style tables before
        extraction. Default is ``None``.
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
        """Validate entity grains, aggregation, and time-varying consistency.

        Raises
        ------
        ValueError
            If aggregation is invalid, entity_type is unknown, entity grain
            is not a subset of grain/resolved_grain, or time-varying
            consistency rules are violated.
        """
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
        """Return identity columns for the entity (facility, edge, or resource category).

        Returns
        -------
        tuple of str
            Column names that identify the entity.
        """
        return _ENTITY_GRAINS[self.entity_type]

    @property
    def time_varying(self) -> bool:
        """Check whether raw grain includes calendar ``date``.

        Returns
        -------
        bool
            ``True`` when the attribute is time-varying.
        """
        return "date" in self.grain

    def resolved_merge_grain(self) -> tuple[str, ...]:
        """Return grain columns used when merging resolved attribute tables.

        Returns
        -------
        tuple of str
            Column names forming the merge grain after time resolution.
        """
        return self.resolved_grain
