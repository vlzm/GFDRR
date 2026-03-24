"""AttributeRegistry: central API for registering parametric attributes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import pandas as pd

from gbp.core.attributes.builder import _validate_numeric_series
from gbp.core.attributes.spec import AttributeSpec
from gbp.core.enums import AttributeKind


@dataclass(frozen=True)
class RegisteredAttribute:
    """Attribute spec paired with its data — stored together."""

    spec: AttributeSpec
    data: pd.DataFrame


class AttributeRegistry:
    """Registry of parametric attributes for a model.

    Central API for adding, validating, and retrieving parameter tables
    with their grain definitions.
    """

    def __init__(self) -> None:
        self._attributes: dict[str, RegisteredAttribute] = {}

    # ── registration ──────────────────────────────────────────────────

    def register(
        self,
        name: str,
        data: pd.DataFrame,
        *,
        entity_type: str,
        kind: AttributeKind,
        grain: tuple[str, ...],
        value_column: str,
        aggregation: str = "mean",
        unit: str | None = None,
        nullable: bool = True,
        eav_filter: Mapping[str, Any] | None = None,
    ) -> None:
        """Register a parametric attribute with its data and grain.

        Validates grain columns are present in *data*, value column exists,
        and numeric values satisfy kind constraints (COST >= 0, CAPACITY > 0).

        Raises:
            ValueError: on validation failure.
        """
        if name in self._attributes:
            raise ValueError(
                f"Attribute {name!r} is already registered. "
                f"Use a unique name for each attribute."
            )

        resolved_grain = tuple(
            "period_id" if g == "date" else g for g in grain
        )

        spec = AttributeSpec(
            name=name,
            kind=kind,
            entity_type=entity_type,
            grain=grain,
            resolved_grain=resolved_grain,
            value_column=value_column,
            source_table=name,
            unit=unit,
            aggregation=aggregation,
            nullable=nullable,
            eav_filter=eav_filter,
        )

        required_cols = set(grain) | {value_column}
        if eav_filter:
            required_cols |= set(eav_filter.keys())
        missing = required_cols - set(data.columns)
        if missing:
            raise ValueError(
                f"Attribute {name!r}: data missing columns {missing}; "
                f"have {list(data.columns)}"
            )

        if not data.empty:
            _validate_numeric_series(spec, data[value_column])
        elif not nullable:
            raise ValueError(
                f"Attribute {name!r}: data is empty but nullable=False. "
                f"Provide at least one row or set nullable=True."
            )

        self._attributes[name] = RegisteredAttribute(spec=spec, data=data)

    def register_raw(self, spec: AttributeSpec, data: pd.DataFrame) -> None:
        """Register with a pre-built spec (used for resolved / deserialized data)."""
        self._attributes[spec.name] = RegisteredAttribute(spec=spec, data=data)

    # ── lookup ────────────────────────────────────────────────────────

    def get(self, name: str) -> RegisteredAttribute:
        """Get attribute by name. Raises ``KeyError`` if not found."""
        return self._attributes[name]

    def get_by_entity(self, entity_type: str) -> list[RegisteredAttribute]:
        """All attributes for a given entity type."""
        return [a for a in self._attributes.values() if a.spec.entity_type == entity_type]

    def get_by_kind(self, kind: AttributeKind) -> list[RegisteredAttribute]:
        """All attributes of a given kind (e.g. all COSTs)."""
        return [a for a in self._attributes.values() if a.spec.kind == kind]

    # ── properties ────────────────────────────────────────────────────

    @property
    def specs(self) -> list[AttributeSpec]:
        """All registered specs (for build pipeline)."""
        return [a.spec for a in self._attributes.values()]

    @property
    def names(self) -> list[str]:
        """All registered attribute names."""
        return list(self._attributes.keys())

    def __len__(self) -> int:
        return len(self._attributes)

    def __contains__(self, name: str) -> bool:
        return name in self._attributes

    def __bool__(self) -> bool:
        return bool(self._attributes)

    # ── serialization helpers ─────────────────────────────────────────

    def to_dict(self) -> dict[str, pd.DataFrame]:
        """All attribute data as ``{name: DataFrame}``."""
        return {name: a.data for name, a in self._attributes.items()}

    def copy(self) -> AttributeRegistry:
        """Shallow copy — specs are frozen, DataFrames are shared."""
        new = AttributeRegistry()
        new._attributes = dict(self._attributes)
        return new

    # ── display ───────────────────────────────────────────────────────

    def summary(self) -> str:
        """Human-readable summary of registered attributes."""
        if not self._attributes:
            return "  (no attributes registered)"
        lines: list[str] = []
        for name, attr in self._attributes.items():
            s = attr.spec
            grain_str = " × ".join(s.grain)
            lines.append(
                f"    {name}: {len(attr.data)} rows  "
                f"{s.entity_type}  {s.kind.value.upper()}  "
                f"[{grain_str}]"
            )
        return "\n".join(lines)

    def __repr__(self) -> str:
        return f"AttributeRegistry({len(self._attributes)} attributes)"
