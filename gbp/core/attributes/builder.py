"""Assemble spine tables from base entities and attribute DataFrames."""

from __future__ import annotations

import pandas as pd

from gbp.core.attributes.grain_groups import auto_group_attributes
from gbp.core.attributes.merge_plan import plan_merges
from gbp.core.attributes.spec import _ENTITY_GRAINS, AttributeSpec
from gbp.core.enums import AttributeKind


def _validate_numeric_series(spec: AttributeSpec, series: pd.Series) -> None:
    """Raise ``ValueError`` if values violate kind constraints.

    Parameters
    ----------
    spec
        Attribute specification whose ``kind`` determines constraints.
    series
        Value series to validate.

    Raises
    ------
    ValueError
        If all non-null values fail numeric coercion, COST/REVENUE/RATE
        values are negative, or CAPACITY values are non-positive.
    """
    if series.empty:
        return
    # Distinguish "all values are None/NaN" (legitimate for nullable) from
    # "all non-null values failed numeric coercion" (always an error).
    non_null = series.dropna()
    if non_null.empty:
        return
    numeric = pd.to_numeric(non_null, errors="coerce")
    mask = numeric.notna()
    if not mask.any():
        raise ValueError(
            f"Attribute {spec.name!r}: value column contains no numeric values "
            f"(all {len(non_null)} non-null values coerced to NaN)"
        )
    v = numeric[mask]

    if spec.kind in (AttributeKind.COST, AttributeKind.REVENUE, AttributeKind.RATE):
        if (v < 0).any():
            raise ValueError(
                f"Attribute {spec.name!r} (kind={spec.kind.value}) has negative values"
            )
    elif spec.kind == AttributeKind.CAPACITY:
        if (v <= 0).any():
            raise ValueError(
                f"Attribute {spec.name!r} (kind={spec.kind.value}) has non-positive values"
            )
    # ADDITIONAL: no automatic constraint


def _validate_grain_columns(spec: AttributeSpec, df: pd.DataFrame) -> None:
    """Verify that *df* contains all grain and value columns required by *spec*.

    Parameters
    ----------
    spec
        Attribute specification defining required columns.
    df
        DataFrame to check.

    Raises
    ------
    ValueError
        If any required column is missing from *df*.
    """
    need = list(spec.resolved_merge_grain()) + [spec.value_column]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise ValueError(
            f"Attribute {spec.name!r}: data missing columns {missing}; have {list(df.columns)}"
        )


def _prepare_attribute_frame(spec: AttributeSpec, df: pd.DataFrame) -> pd.DataFrame:
    """Filter EAV rows and select merge columns with output named ``spec.name``.

    Parameters
    ----------
    spec
        Attribute specification controlling EAV filtering, grain selection,
        and numeric validation.
    df
        Raw attribute data.

    Returns
    -------
    pd.DataFrame
        Deduplicated frame with grain columns and the value column renamed
        to ``spec.name``. May be empty if the EAV filter eliminates all rows.

    Raises
    ------
    ValueError
        If an ``eav_filter`` key is not present in *df* columns, or if
        required grain/value columns are missing.
    """
    out = df.copy()
    if spec.eav_filter:
        for col, val in spec.eav_filter.items():
            if col not in out.columns:
                raise ValueError(
                    f"Attribute {spec.name!r}: eav_filter key {col!r} not in data columns"
                )
            out = out.loc[out[col] == val]
    if out.empty:
        return out
    _validate_grain_columns(spec, out)
    cols = list(spec.resolved_merge_grain()) + [spec.value_column]
    out = out[cols].drop_duplicates()
    if not out.empty:
        _validate_numeric_series(spec, out[spec.value_column])
    return out.rename(columns={spec.value_column: spec.name})


class AttributeBuilder:
    """Register ``AttributeSpec`` rows and build one spine per grain group.

    Parameters
    ----------
    entity_type
        Entity kind this builder accepts: ``"facility"``, ``"edge"``,
        or ``"resource"``.

    Raises
    ------
    ValueError
        If *entity_type* is not one of the known entity kinds.
    """

    def __init__(self, entity_type: str) -> None:  # noqa: D107
        if entity_type not in _ENTITY_GRAINS:
            raise ValueError(
                f"entity_type must be one of {sorted(_ENTITY_GRAINS)}, got {entity_type!r}"
            )
        self._entity_type = entity_type
        self._entity_grain: list[str] = list(_ENTITY_GRAINS[entity_type])
        self._attributes: list[AttributeSpec] = []

    @property
    def entity_type(self) -> str:
        """Return entity kind this builder accepts.

        Returns
        -------
        str
            One of ``"facility"``, ``"edge"``, ``"resource"``.
        """
        return self._entity_type

    @property
    def entity_grain(self) -> list[str]:
        """Return identity column names for the entity base table.

        Returns
        -------
        list of str
            Copy of the entity grain column names.
        """
        return list(self._entity_grain)

    def register(self, spec: AttributeSpec) -> None:
        """Register a spec; must match this builder's entity type and grains.

        Parameters
        ----------
        spec
            Attribute specification to add.

        Raises
        ------
        ValueError
            If *spec.entity_type* or *spec.entity_grain* does not match
            this builder.
        """
        if spec.entity_type != self._entity_type:
            raise ValueError(
                f"Spec {spec.name!r} has entity_type={spec.entity_type!r}, "
                f"expected {self._entity_type!r}"
            )
        eg = set(spec.entity_grain)
        if set(self._entity_grain) != eg:
            raise ValueError(
                f"Spec {spec.name!r}: entity_grain {spec.entity_grain!r} "
                f"does not match builder grain {tuple(self._entity_grain)!r}"
            )
        self._attributes.append(spec)

    def build_spines(
        self,
        base_df: pd.DataFrame,
        attribute_data: dict[str, pd.DataFrame],
    ) -> dict[str, pd.DataFrame]:
        """Build one spine DataFrame per grain group.

        Parameters
        ----------
        base_df
            Entity base table containing at least the entity grain columns.
        attribute_data
            Mapping from attribute name to its raw data DataFrame.

        Returns
        -------
        dict of str to pd.DataFrame
            Mapping from group name to the assembled spine DataFrame.

        Raises
        ------
        ValueError
            If *base_df* is missing entity grain columns, required attribute
            data is absent, or merge keys cannot be resolved.
        """
        if not self._attributes:
            return {}

        missing_base = [c for c in self._entity_grain if c not in base_df.columns]
        if missing_base:
            raise ValueError(
                f"base_df missing entity grain columns {missing_base}; "
                f"have {list(base_df.columns)}"
            )

        groups = auto_group_attributes(self._entity_grain, self._attributes)
        out: dict[str, pd.DataFrame] = {}

        for group in groups:
            spine = base_df.copy()
            plans = plan_merges(self._entity_grain, group.attributes)
            spec_by_name = {s.name: s for s in group.attributes}

            for plan in plans:
                spec = spec_by_name[plan.attribute_name]
                raw_df = attribute_data.get(spec.name)
                if raw_df is None or raw_df.empty:
                    if not spec.nullable:
                        raise ValueError(f"Missing required attribute data: {spec.name!r}")
                    continue

                right = _prepare_attribute_frame(spec, raw_df)
                if right.empty:
                    if not spec.nullable:
                        raise ValueError(
                            f"Attribute {spec.name!r}: no rows after EAV filter / selection"
                        )
                    continue

                merge_keys = [
                    k
                    for k in spec.resolved_merge_grain()
                    if k in spine.columns and k in right.columns
                ]
                if not merge_keys:
                    raise ValueError(
                        f"Attribute {spec.name!r}: no join keys shared between spine "
                        f"{list(spine.columns)} and right {list(right.columns)}. "
                        f"Expected merge on resolved_grain={spec.resolved_merge_grain()}. "
                        f"Common causes: grain mismatch between spec and data, "
                        f"or missing time resolution (date→period_id)."
                    )

                spine = spine.merge(right, on=merge_keys, how="left")

            out[group.name] = spine

        return out
