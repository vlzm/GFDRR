"""Spine assembly: merge resolved attribute tables onto entity bases (§13.7)."""

from __future__ import annotations

import pandas as pd

from gbp.core.attributes import AttributeBuilder, AttributeSpec
from gbp.core.attributes.defaults import get_structural_attribute_specs
from gbp.core.model import ResolvedModelData


def _normalize_resource_categories(df: pd.DataFrame) -> pd.DataFrame:
    """Use ``resource_category`` as the FK column name for resource spines."""
    out = df.copy()
    if "resource_category_id" in out.columns and "resource_category" not in out.columns:
        out = out.rename(columns={"resource_category_id": "resource_category"})
    return out


def _load_attribute_table(resolved: ResolvedModelData, spec: AttributeSpec) -> pd.DataFrame | None:
    """Return a copy of the source table with resource FK naming aligned."""
    df = getattr(resolved, spec.source_table, None)
    if df is None or df.empty:
        return None
    if spec.source_table == "resource_categories":
        return _normalize_resource_categories(df)
    return df.copy()


def _maybe_put_attr_data(
    attr_data: dict[str, pd.DataFrame],
    resolved: ResolvedModelData,
    spec: AttributeSpec,
) -> None:
    """Store attribute data if the source table and value column are available."""
    t = _load_attribute_table(resolved, spec)
    if t is None or t.empty:
        return
    if spec.value_column not in t.columns:
        if not spec.nullable:
            raise ValueError(
                f"Attribute {spec.name!r}: column {spec.value_column!r} "
                f"not in table {spec.source_table!r}"
            )
        return
    if spec.time_varying and "period_id" not in t.columns:
        if not spec.nullable:
            raise ValueError(
                f"Attribute {spec.name!r}: time-varying data in {spec.source_table!r} "
                f"has no period_id (run time resolution first)"
            )
        return
    if spec.eav_filter:
        for col in spec.eav_filter:
            if col not in t.columns:
                if not spec.nullable:
                    raise ValueError(
                        f"Attribute {spec.name!r}: eav_filter column {col!r} "
                        f"not in table {spec.source_table!r}"
                    )
                return
    attr_data[spec.name] = t


def _collect_all_specs(
    resolved: ResolvedModelData,
    attribute_specs: list[AttributeSpec] | None,
) -> list[AttributeSpec]:
    """Combine registry specs, explicit specs, and structural defaults.

    Priority:
    1. Explicit ``attribute_specs`` (if given) are used as-is.
    2. Otherwise: registry specs + structural defaults for any entity
       that has no registry coverage.
    """
    if attribute_specs is not None:
        return attribute_specs

    registry = resolved.attributes
    if registry:
        specs = list(registry.specs)
        seen_names = {s.name for s in specs}
        for s in get_structural_attribute_specs():
            if s.name not in seen_names:
                specs.append(s)
                seen_names.add(s.name)
        return specs

    return get_structural_attribute_specs()


def _collect_attr_data(
    resolved: ResolvedModelData,
    specs: list[AttributeSpec],
) -> dict[str, pd.DataFrame]:
    """Gather attribute data from registry and structural tables."""
    attr_data: dict[str, pd.DataFrame] = {}
    registry = resolved.attributes

    for spec in specs:
        if registry and spec.name in registry:
            data = registry.get(spec.name).data
            if data is not None and not data.empty:
                attr_data[spec.name] = data
        else:
            _maybe_put_attr_data(attr_data, resolved, spec)

    return attr_data


def assemble_spines(
    resolved: ResolvedModelData,
    attribute_specs: list[AttributeSpec] | None = None,
) -> dict[str, dict[str, pd.DataFrame]]:
    """Build per-entity spine dicts keyed by grain-group name.

    When ``attribute_specs`` is *None*, specs are drawn from the
    ``resolved.attributes`` registry (with structural defaults added
    for any missing entity coverage), falling back to the full legacy
    default catalog when the registry is empty.

    Returns:
        ``{"facility": {group: df, ...}, "edge": {...}, "resource": {...}}``.
        Missing entity bases (e.g. no ``edges``) yield empty inner dicts.
    """
    specs = _collect_all_specs(resolved, attribute_specs)
    by_entity: dict[str, list[AttributeSpec]] = {"facility": [], "edge": [], "resource": []}
    for s in specs:
        by_entity[s.entity_type].append(s)

    attr_data = _collect_attr_data(resolved, specs)

    out: dict[str, dict[str, pd.DataFrame]] = {
        "facility": {},
        "edge": {},
        "resource": {},
    }

    # --- facility ---
    f_specs = by_entity["facility"]
    if f_specs:
        base_f = resolved.facilities.copy()
        f_data = {k: v for k, v in attr_data.items() if k in {s.name for s in f_specs}}
        b = AttributeBuilder("facility")
        for s in f_specs:
            b.register(s)
        out["facility"] = b.build_spines(base_f, f_data)

    # --- edge ---
    e_specs = by_entity["edge"]
    if e_specs and resolved.edges is not None and not resolved.edges.empty:
        base_e = resolved.edges.copy()
        e_data = {k: v for k, v in attr_data.items() if k in {s.name for s in e_specs}}
        eb = AttributeBuilder("edge")
        for s in e_specs:
            eb.register(s)
        out["edge"] = eb.build_spines(base_e, e_data)

    # --- resource ---
    r_specs = by_entity["resource"]
    if (
        r_specs
        and resolved.resource_categories is not None
        and not resolved.resource_categories.empty
    ):
        base_r = _normalize_resource_categories(resolved.resource_categories.copy())
        r_data = {k: v for k, v in attr_data.items() if k in {s.name for s in r_specs}}
        rb = AttributeBuilder("resource")
        for s in r_specs:
            rb.register(s)
        out["resource"] = rb.build_spines(base_r, r_data)

    return out
