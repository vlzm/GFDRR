"""Dict/JSON serialization for ``RawModelData`` and ``ResolvedModelData``."""

from __future__ import annotations

from dataclasses import fields
from typing import Any

import pandas as pd

from gbp.core.model import RawModelData, ResolvedModelData

_SPINE_FIELDS = frozenset({"facility_spines", "edge_spines", "resource_spines"})
_SKIP_PREFIXES = ("_",)

_DATE_COLUMNS = frozenset({
    "date",
    "start_date",
    "end_date",
    "departure_date",
    "expected_arrival_date",
})


def _df_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Serialize a DataFrame to list-of-dicts with dates as ISO strings."""
    out = df.copy()
    for col in out.columns:
        if col in _DATE_COLUMNS or pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].astype(str)
    return out.to_dict(orient="records")


def _records_to_df(records: list[dict[str, Any]]) -> pd.DataFrame:
    """Reconstruct a DataFrame, parsing known date columns."""
    df = pd.DataFrame(records)
    for col in df.columns:
        if col in _DATE_COLUMNS:
            try:
                df[col] = pd.to_datetime(df[col]).dt.date
            except (ValueError, TypeError):
                pass
    return df


def _to_dict(obj: RawModelData | ResolvedModelData) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for f in fields(obj):
        if any(f.name.startswith(p) for p in _SKIP_PREFIXES):
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
    return result


def _from_dict(
    data: dict[str, Any],
    cls: type,
    required: frozenset[str],
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    field_names = {f.name for f in fields(cls)}

    for name, value in data.items():
        if name not in field_names:
            continue
        if name in _SPINE_FIELDS:
            kwargs[name] = {k: _records_to_df(v) for k, v in value.items()}
            continue
        if isinstance(value, list):
            kwargs[name] = _records_to_df(value)

    for r in required:
        if r not in kwargs:
            raise ValueError(f"Required table {r!r} missing from dict data")
    return kwargs


# -- public API ----------------------------------------------------------------


def raw_to_dict(raw: RawModelData) -> dict[str, Any]:
    """Serialize ``RawModelData`` to a JSON-compatible dict."""
    return _to_dict(raw)


def raw_from_dict(data: dict[str, Any]) -> RawModelData:
    """Reconstruct ``RawModelData`` from a dict (inverse of ``raw_to_dict``)."""
    kwargs = _from_dict(data, RawModelData, RawModelData._REQUIRED)
    raw = RawModelData(**kwargs)
    raw.validate()
    return raw


def resolved_to_dict(resolved: ResolvedModelData) -> dict[str, Any]:
    """Serialize ``ResolvedModelData`` to a JSON-compatible dict."""
    return _to_dict(resolved)


def resolved_from_dict(data: dict[str, Any], *, validate: bool = False) -> ResolvedModelData:
    """Reconstruct ``ResolvedModelData`` from a dict.

    Validation is off by default because resolved tables may have different
    column sets after time resolution (e.g. ``period_id`` replaces ``date``).
    """
    kwargs = _from_dict(data, ResolvedModelData, ResolvedModelData._REQUIRED)
    resolved = ResolvedModelData(**kwargs)
    if validate:
        resolved.validate()
    return resolved
