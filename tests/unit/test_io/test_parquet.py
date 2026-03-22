"""Tests for Parquet serialization of model data."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from gbp.build.pipeline import build_model
from gbp.io.parquet import (
    load_raw_parquet,
    load_resolved_parquet,
    save_raw_parquet,
    save_resolved_parquet,
)
from tests.unit.build.fixtures import minimal_raw_model


def test_raw_round_trip(tmp_path: Path) -> None:
    """Save and reload RawModelData; DataFrames must match."""
    raw = minimal_raw_model(with_edges=True, with_demand=True)
    save_raw_parquet(raw, tmp_path / "raw")
    loaded = load_raw_parquet(tmp_path / "raw")

    for name in ("facilities", "commodity_categories", "periods", "edge_rules"):
        orig = getattr(raw, name)
        got = getattr(loaded, name)
        pd.testing.assert_frame_equal(orig, got, check_dtype=False)


def test_resolved_round_trip(tmp_path: Path) -> None:
    """Save and reload ResolvedModelData including spine dicts."""
    raw = minimal_raw_model(with_edges=True, with_demand=True)
    resolved = build_model(raw)
    save_resolved_parquet(resolved, tmp_path / "resolved")
    loaded = load_resolved_parquet(tmp_path / "resolved")

    for name in ("facilities", "periods", "edge_rules"):
        pd.testing.assert_frame_equal(
            getattr(resolved, name),
            getattr(loaded, name),
            check_dtype=False,
        )

    if resolved.resource_spines:
        assert loaded.resource_spines is not None
        for gn, df in resolved.resource_spines.items():
            pd.testing.assert_frame_equal(df, loaded.resource_spines[gn], check_dtype=False)


def test_raw_parquet_skips_none_tables(tmp_path: Path) -> None:
    """Optional None tables produce no parquet files."""
    raw = minimal_raw_model(with_edges=False, with_demand=False)
    save_raw_parquet(raw, tmp_path / "raw")
    assert not (tmp_path / "raw" / "edges.parquet").exists()
    assert not (tmp_path / "raw" / "demand.parquet").exists()


def test_metadata_json_structure(tmp_path: Path) -> None:
    """``_metadata.json`` has expected keys."""
    raw = minimal_raw_model(with_edges=True)
    save_raw_parquet(raw, tmp_path / "raw")

    with open(tmp_path / "raw" / "_metadata.json") as f:
        meta = json.load(f)
    assert meta["format"] == "RawModelData"
    assert meta["version"] == 1
    assert "facilities" in meta["tables"]


def test_load_missing_required_raises(tmp_path: Path) -> None:
    """Loading without a required Parquet file raises FileNotFoundError."""
    raw = minimal_raw_model()
    save_raw_parquet(raw, tmp_path / "raw")

    (tmp_path / "raw" / "facilities.parquet").unlink()

    with open(tmp_path / "raw" / "_metadata.json") as f:
        meta = json.load(f)
    meta["tables"] = [t for t in meta["tables"] if t != "facilities"]
    with open(tmp_path / "raw" / "_metadata.json", "w") as f:
        json.dump(meta, f)

    with pytest.raises(FileNotFoundError, match="facilities"):
        load_raw_parquet(tmp_path / "raw")
