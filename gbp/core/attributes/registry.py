"""Central API for registering parametric attributes."""

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
    """Pair an attribute spec with its data for joint storage.

    Parameters
    ----------
    spec
        Attribute specification.
    data
        DataFrame holding the attribute values.
    """

    spec: AttributeSpec
    data: pd.DataFrame


class AttributeRegistry:
    """Provide a central API for parametric attribute registration.

    Store, validate, and retrieve parameter tables together with their
    grain definitions.
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

        Validate that grain columns are present in *data*, value column
        exists, and numeric values satisfy kind constraints (COST >= 0,
        CAPACITY > 0).

        Parameters
        ----------
        name
            Unique attribute name.
        data
            DataFrame containing the attribute values and grain columns.
        entity_type
            One of ``"facility"``, ``"edge"``, ``"resource"``.
        kind
            Semantic kind (cost, capacity, rate, revenue, additional).
        grain
            Raw grain column names (use ``"date"`` for time-varying).
        value_column
            Column in *data* holding the attribute value.
        aggregation
            Aggregation function name. Default is ``"mean"``.
        unit
            Physical unit string. Default is ``None``.
        nullable
            Whether missing values are allowed. Default is ``True``.
        eav_filter
            Column-value pairs for EAV-style row filtering. Default is
            ``None``.

        Raises
        ------
        ValueError
            If *name* is already registered, required columns are missing
            from *data*, numeric constraints are violated, or *data* is
            empty with ``nullable=False``.
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
        """Register with a pre-built spec.

        Intended for resolved or deserialized data that already has a
        fully constructed ``AttributeSpec``.

        Parameters
        ----------
        spec
            Pre-built attribute specification.
        data
            DataFrame holding the attribute values.
        """
        self._attributes[spec.name] = RegisteredAttribute(spec=spec, data=data)

    # ── lookup ────────────────────────────────────────────────────────

    def get(self, name: str) -> RegisteredAttribute:
        """Return attribute by name.

        Parameters
        ----------
        name
            Registered attribute name.

        Returns
        -------
        RegisteredAttribute
            Spec and data pair.

        Raises
        ------
        KeyError
            If *name* is not registered.
        """
        return self._attributes[name]

    def get_by_entity(self, entity_type: str) -> list[RegisteredAttribute]:
        """Return all attributes for a given entity type.

        Parameters
        ----------
        entity_type
            Entity kind to filter by.

        Returns
        -------
        list of RegisteredAttribute
            Matching attributes (may be empty).
        """
        return [a for a in self._attributes.values() if a.spec.entity_type == entity_type]

    def get_by_kind(self, kind: AttributeKind) -> list[RegisteredAttribute]:
        """Return all attributes of a given kind.

        Parameters
        ----------
        kind
            Attribute kind to filter by (e.g. ``AttributeKind.COST``).

        Returns
        -------
        list of RegisteredAttribute
            Matching attributes (may be empty).
        """
        return [a for a in self._attributes.values() if a.spec.kind == kind]

    # ── properties ────────────────────────────────────────────────────

    @property
    def specs(self) -> list[AttributeSpec]:
        """Return all registered specs.

        Returns
        -------
        list of AttributeSpec
            Specs in insertion order, suitable for the build pipeline.
        """
        return [a.spec for a in self._attributes.values()]

    @property
    def names(self) -> list[str]:
        """Return all registered attribute names.

        Returns
        -------
        list of str
            Names in insertion order.
        """
        return list(self._attributes.keys())

    def __len__(self) -> int:
        """Return the number of registered attributes."""
        return len(self._attributes)

    def __contains__(self, name: str) -> bool:
        """Check whether *name* is registered."""
        return name in self._attributes

    def __bool__(self) -> bool:
        """Return ``True`` if any attributes are registered."""
        return bool(self._attributes)

    # ── serialization helpers ─────────────────────────────────────────

    def to_dict(self) -> dict[str, pd.DataFrame]:
        """Return all attribute data as a name-to-DataFrame mapping.

        Returns
        -------
        dict of str to pd.DataFrame
            Attribute data keyed by name.
        """
        return {name: a.data for name, a in self._attributes.items()}

    def copy(self) -> AttributeRegistry:
        """Return a shallow copy of the registry.

        Specs are frozen dataclasses; DataFrames are shared, not copied.

        Returns
        -------
        AttributeRegistry
            New registry with the same entries.
        """
        new = AttributeRegistry()
        new._attributes = dict(self._attributes)
        return new

    # ── display ───────────────────────────────────────────────────────

    def summary(self) -> str:
        """Return a human-readable summary of registered attributes.

        Returns
        -------
        str
            Multi-line summary with one line per attribute showing row
            count, entity type, kind, and grain.
        """
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
        """Return developer-friendly string representation."""
        return f"AttributeRegistry({len(self._attributes)} attributes)"
