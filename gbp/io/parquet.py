"""Parquet serialization for ``RawModelData`` and ``ResolvedModelData``."""

from __future__ import annotations

import json
from dataclasses import fields
from pathlib import Path
from typing import Any

import pandas as pd

from gbp.core.attributes.registry import AttributeRegistry
from gbp.core.attributes.spec import AttributeSpec
from gbp.core.enums import AttributeKind
from gbp.core.model import RawModelData, ResolvedModelData

_SPINE_FIELDS = frozenset({"facility_spines", "edge_spines", "resource_spines"})
_NON_TABLE_FIELDS = frozenset({"attributes"})
_SKIP_PREFIXES = ("_",)


def _is_dataclass_df_field(name: str, obj: object) -> bool:
    val = getattr(obj, name, None)
    return isinstance(val, pd.DataFrame)


def _save_attribute_registry(
    registry: AttributeRegistry,
    directory: Path,
) -> dict[str, Any]:
    """Serialize attribute registry to an ``attributes/`` sub-directory."""
    if not registry:
        return {}

    attr_dir = directory / "attributes"
    attr_dir.mkdir(exist_ok=True)

    specs_list: list[dict[str, Any]] = []
    for attr in registry.specs:
        spec_dict: dict[str, Any] = {
            "name": attr.name,
            "kind": attr.kind.value,
            "entity_type": attr.entity_type,
            "grain": list(attr.grain),
            "resolved_grain": list(attr.resolved_grain),
            "value_column": attr.value_column,
            "source_table": attr.source_table,
            "aggregation": attr.aggregation,
            "nullable": attr.nullable,
        }
        if attr.unit is not None:
            spec_dict["unit"] = attr.unit
        if attr.eav_filter:
            spec_dict["eav_filter"] = dict(attr.eav_filter)
        specs_list.append(spec_dict)

        data = registry.get(attr.name).data
        data.to_parquet(attr_dir / f"{attr.name}.parquet", index=False)

    with open(attr_dir / "_specs.json", "w") as fh:
        json.dump(specs_list, fh, indent=2)

    return {"attribute_names": [s["name"] for s in specs_list]}


def _load_attribute_registry(directory: Path) -> AttributeRegistry:
    """Deserialize attribute registry from an ``attributes/`` sub-directory."""
    registry = AttributeRegistry()
    attr_dir = directory / "attributes"

    if not attr_dir.exists():
        return registry

    specs_path = attr_dir / "_specs.json"
    if not specs_path.exists():
        return registry

    with open(specs_path) as fh:
        specs_list = json.load(fh)

    for spec_dict in specs_list:
        name = spec_dict["name"]
        pq = attr_dir / f"{name}.parquet"
        if not pq.exists():
            continue

        spec = AttributeSpec(
            name=name,
            kind=AttributeKind(spec_dict["kind"]),
            entity_type=spec_dict["entity_type"],
            grain=tuple(spec_dict["grain"]),
            resolved_grain=tuple(spec_dict["resolved_grain"]),
            value_column=spec_dict["value_column"],
            source_table=spec_dict["source_table"],
            unit=spec_dict.get("unit"),
            aggregation=spec_dict.get("aggregation", "mean"),
            nullable=spec_dict.get("nullable", True),
            eav_filter=spec_dict.get("eav_filter"),
        )
        data = pd.read_parquet(pq)
        registry.register_raw(spec, data)

    return registry


def _save_tables(
    obj: RawModelData | ResolvedModelData,
    directory: Path,
    format_name: str,
) -> None:
    """Write all non-None DataFrames as Parquet and produce ``_metadata.json``."""
    directory.mkdir(parents=True, exist_ok=True)
    tables: list[str] = []
    spine_meta: dict[str, list[str]] = {}

    for f in fields(obj):
        if any(f.name.startswith(p) for p in _SKIP_PREFIXES):
            continue
        if f.name in _NON_TABLE_FIELDS:
            continue

        if f.name in _SPINE_FIELDS:
            spine_dict = getattr(obj, f.name)
            if spine_dict is None:
                continue
            sub = directory / f.name
            sub.mkdir(exist_ok=True)
            group_names: list[str] = []
            for group_name, df in spine_dict.items():
                df.to_parquet(sub / f"{group_name}.parquet", index=False)
                group_names.append(group_name)
            spine_meta[f.name] = group_names
            continue

        val = getattr(obj, f.name)
        if val is None or not isinstance(val, pd.DataFrame):
            continue
        val.to_parquet(directory / f"{f.name}.parquet", index=False)
        tables.append(f.name)

    attr_meta = _save_attribute_registry(obj.attributes, directory)

    meta: dict[str, Any] = {
        "format": format_name,
        "version": 1,
        "tables": tables,
    }
    if spine_meta:
        meta["spines"] = spine_meta
    if attr_meta:
        meta["attributes"] = attr_meta

    with open(directory / "_metadata.json", "w") as fh:
        json.dump(meta, fh, indent=2)


def _load_tables(
    directory: Path,
    cls: type,
    required: frozenset[str],
) -> dict[str, Any]:
    """Read Parquet files back into constructor kwargs for *cls*."""
    meta_path = directory / "_metadata.json"
    if not meta_path.exists():
        raise FileNotFoundError(f"Missing _metadata.json in {directory}")

    with open(meta_path) as fh:
        meta = json.load(fh)

    tables: list[str] = meta.get("tables", [])
    kwargs: dict[str, Any] = {}
    field_names = {f.name for f in fields(cls)}

    for name in tables:
        pq = directory / f"{name}.parquet"
        if not pq.exists():
            if name in required:
                raise FileNotFoundError(f"Required table {name}.parquet not found in {directory}")
            continue
        if name not in field_names:
            continue
        kwargs[name] = pd.read_parquet(pq)

    for name in required:
        if name not in kwargs:
            pq = directory / f"{name}.parquet"
            if pq.exists():
                kwargs[name] = pd.read_parquet(pq)
            else:
                raise FileNotFoundError(
                    f"Required table {name}.parquet not found in {directory}"
                )

    spine_meta: dict[str, list[str]] = meta.get("spines", {})
    for spine_field, group_names in spine_meta.items():
        if spine_field not in field_names:
            continue
        sub = directory / spine_field
        if not sub.exists():
            continue
        spine_dict: dict[str, pd.DataFrame] = {}
        for gn in group_names:
            gpq = sub / f"{gn}.parquet"
            if gpq.exists():
                spine_dict[gn] = pd.read_parquet(gpq)
        kwargs[spine_field] = spine_dict if spine_dict else None

    if "attributes" in field_names:
        kwargs["attributes"] = _load_attribute_registry(directory)

    return kwargs


# -- public API ----------------------------------------------------------------


def save_raw_parquet(raw: RawModelData, directory: str | Path) -> None:
    """Save ``RawModelData`` as a directory of Parquet files."""
    _save_tables(raw, Path(directory), "RawModelData")


def load_raw_parquet(directory: str | Path) -> RawModelData:
    """Load ``RawModelData`` from a Parquet directory."""
    kwargs = _load_tables(Path(directory), RawModelData, RawModelData._REQUIRED)
    raw = RawModelData(**kwargs)
    raw.validate()
    return raw


def save_resolved_parquet(resolved: ResolvedModelData, directory: str | Path) -> None:
    """Save ``ResolvedModelData`` as a directory of Parquet files."""
    _save_tables(resolved, Path(directory), "ResolvedModelData")


def load_resolved_parquet(
    directory: str | Path,
    *,
    validate: bool = False,
) -> ResolvedModelData:
    """Load ``ResolvedModelData`` from a Parquet directory.

    Validation is off by default because resolved tables may have different
    column sets after time resolution (e.g. ``period_id`` replaces ``date``).
    """
    kwargs = _load_tables(
        Path(directory), ResolvedModelData, ResolvedModelData._REQUIRED
    )
    resolved = ResolvedModelData(**kwargs)
    if validate:
        resolved.validate()
    return resolved
