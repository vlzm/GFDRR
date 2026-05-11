"""Dict/JSON serialization for ``RawModelData`` and ``ResolvedModelData``."""

from __future__ import annotations

from dataclasses import fields
from typing import Any

import pandas as pd

from gbp.core.attributes.registry import AttributeRegistry
from gbp.core.attributes.spec import AttributeSpec
from gbp.core.enums import AttributeKind
from gbp.core.model import RawModelData, ResolvedModelData

_SPINE_FIELDS = frozenset({"facility_spines", "edge_spines", "resource_spines"})
_NON_TABLE_FIELDS = frozenset({"attributes"})
_SKIP_PREFIXES = ("_",)

_DATE_COLUMNS = frozenset({
    "date",
    "start_date",
    "end_date",
    "departure_date",
    "expected_arrival_date",
})


def _df_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Serialize a DataFrame to list-of-dicts with dates as ISO strings.

    Parameters
    ----------
    df
        DataFrame to serialize.

    Returns
    -------
    list[dict[str, Any]]
        Each dict represents one row with date columns cast to ISO strings.
    """
    out = df.copy()
    for col in out.columns:
        if col in _DATE_COLUMNS or pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].astype(str)
    return out.to_dict(orient="records")


def _records_to_df(records: list[dict[str, Any]]) -> pd.DataFrame:
    """Reconstruct a DataFrame, parsing known date columns.

    Parameters
    ----------
    records
        List of row dicts as produced by ``_df_to_records``.

    Returns
    -------
    pd.DataFrame
        Reconstructed DataFrame with known date columns parsed.
    """
    df = pd.DataFrame(records)
    for col in df.columns:
        if col in _DATE_COLUMNS:
            try:
                df[col] = pd.to_datetime(df[col]).dt.date
            except (ValueError, TypeError):
                pass
    return df


def _registry_to_dict(registry: AttributeRegistry) -> dict[str, Any]:
    """Serialize an AttributeRegistry to a JSON-friendly dict.

    Parameters
    ----------
    registry
        Attribute registry to serialize.

    Returns
    -------
    dict[str, Any]
        Mapping of attribute name to ``{"spec": ..., "data": ...}`` entries.
    """
    if not registry:
        return {}
    result: dict[str, Any] = {}
    for attr_obj in [registry.get(n) for n in registry.names]:
        spec = attr_obj.spec
        spec_dict: dict[str, Any] = {
            "name": spec.name,
            "kind": spec.kind.value,
            "entity_type": spec.entity_type,
            "grain": list(spec.grain),
            "resolved_grain": list(spec.resolved_grain),
            "value_column": spec.value_column,
            "source_table": spec.source_table,
            "aggregation": spec.aggregation,
            "nullable": spec.nullable,
        }
        if spec.unit is not None:
            spec_dict["unit"] = spec.unit
        if spec.eav_filter:
            spec_dict["eav_filter"] = dict(spec.eav_filter)
        result[spec.name] = {
            "spec": spec_dict,
            "data": _df_to_records(attr_obj.data),
        }
    return result


def _registry_from_dict(data: dict[str, Any]) -> AttributeRegistry:
    """Deserialize an AttributeRegistry from a dict.

    Parameters
    ----------
    data
        Dict previously produced by ``_registry_to_dict``.

    Returns
    -------
    AttributeRegistry
        Reconstructed registry with specs and data re-registered.
    """
    registry = AttributeRegistry()
    for name, entry in data.items():
        sd = entry["spec"]
        spec = AttributeSpec(
            name=sd["name"],
            kind=AttributeKind(sd["kind"]),
            entity_type=sd["entity_type"],
            grain=tuple(sd["grain"]),
            resolved_grain=tuple(sd["resolved_grain"]),
            value_column=sd["value_column"],
            source_table=sd["source_table"],
            unit=sd.get("unit"),
            aggregation=sd.get("aggregation", "mean"),
            nullable=sd.get("nullable", True),
            eav_filter=sd.get("eav_filter"),
        )
        df = _records_to_df(entry["data"])
        registry.register_raw(spec, df)
    return registry


def _to_dict(obj: RawModelData | ResolvedModelData) -> dict[str, Any]:
    """Serialize a model dataclass to a JSON-compatible dict.

    Parameters
    ----------
    obj
        ``RawModelData`` or ``ResolvedModelData`` instance.

    Returns
    -------
    dict[str, Any]
        Nested dict with DataFrame fields as record lists.
    """
    result: dict[str, Any] = {}
    for f in fields(obj):
        if any(f.name.startswith(p) for p in _SKIP_PREFIXES):
            continue
        if f.name in _NON_TABLE_FIELDS:
            continue
        if f.name in _SPINE_FIELDS:
            spine = getattr(obj, f.name)
            if spine is not None:
                result[f.name] = {k: _df_to_records(v) for k, v in spine.items()}
            continue
        val = getattr(obj, f.name)
        if val is None:
            continue
        if isinstance(val, pd.DataFrame):
            result[f.name] = _df_to_records(val)

    attr_dict = _registry_to_dict(obj.attributes)
    if attr_dict:
        result["attributes"] = attr_dict

    return result


def _from_dict(
    data: dict[str, Any],
    cls: type,
    required: frozenset[str],
) -> dict[str, Any]:
    """Reconstruct constructor kwargs for a model dataclass from a dict.

    Parameters
    ----------
    data
        Dict previously produced by ``_to_dict``.
    cls
        Target dataclass type (``RawModelData`` or ``ResolvedModelData``).
    required
        Field names that must be present in *data*.

    Returns
    -------
    dict[str, Any]
        Keyword arguments suitable for ``cls(**kwargs)``.

    Raises
    ------
    ValueError
        If a required table is missing from *data*.
    """
    kwargs: dict[str, Any] = {}
    field_names = {f.name for f in fields(cls)}

    for name, value in data.items():
        if name not in field_names:
            continue
        if name in _SPINE_FIELDS:
            kwargs[name] = {k: _records_to_df(v) for k, v in value.items()}
            continue
        if name == "attributes" and isinstance(value, dict):
            kwargs["attributes"] = _registry_from_dict(value)
            continue
        if isinstance(value, list):
            kwargs[name] = _records_to_df(value)

    if "attributes" in field_names and "attributes" not in kwargs:
        kwargs["attributes"] = AttributeRegistry()

    for r in required:
        if r not in kwargs:
            raise ValueError(f"Required table {r!r} missing from dict data")
    return kwargs


# -- public API ----------------------------------------------------------------


def raw_to_dict(raw: RawModelData) -> dict[str, Any]:
    """Serialize ``RawModelData`` to a JSON-compatible dict.

    Parameters
    ----------
    raw
        Raw model data to serialize.

    Returns
    -------
    dict[str, Any]
        JSON-serializable representation of *raw*.
    """
    return _to_dict(raw)


def raw_from_dict(data: dict[str, Any]) -> RawModelData:
    """Reconstruct ``RawModelData`` from a dict (inverse of ``raw_to_dict``).

    Parameters
    ----------
    data
        Dict previously produced by ``raw_to_dict``.

    Returns
    -------
    RawModelData
        Validated raw model data.
    """
    kwargs = _from_dict(data, RawModelData, RawModelData._REQUIRED)
    raw = RawModelData(**kwargs)
    raw.validate()
    return raw


def resolved_to_dict(resolved: ResolvedModelData) -> dict[str, Any]:
    """Serialize ``ResolvedModelData`` to a JSON-compatible dict.

    Parameters
    ----------
    resolved
        Resolved model data to serialize.

    Returns
    -------
    dict[str, Any]
        JSON-serializable representation of *resolved*.
    """
    return _to_dict(resolved)


def resolved_from_dict(data: dict[str, Any], *, validate: bool = False) -> ResolvedModelData:
    """Reconstruct ``ResolvedModelData`` from a dict.

    Validation is off by default because resolved tables may have different
    column sets after time resolution (e.g. ``period_id`` replaces ``date``).

    Parameters
    ----------
    data
        Dict previously produced by ``resolved_to_dict``.
    validate
        Run schema validation on the result. Default is ``False``.

    Returns
    -------
    ResolvedModelData
        Reconstructed resolved model data.
    """
    kwargs = _from_dict(data, ResolvedModelData, ResolvedModelData._REQUIRED)
    resolved = ResolvedModelData(**kwargs)
    if validate:
        resolved.validate()
    return resolved
