"""Parquet round-trip tests for the extended ObservedFlow schema."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd


def test_observed_flow_parquet_null_float_dtype(tmp_path: Path) -> None:
    """All-null ``duration_hours`` survives parquet round-trip as float64.

    Catches a known parquet pitfall where an all-null float column may be
    restored as ``object`` dtype by pyarrow when the writer fails to record
    the originating dtype.  ADR Sec. 7.4 of the historical-replay plan
    requires this dtype invariance for downstream phases that assume
    arithmetic-compatible numeric input.
    """
    df = pd.DataFrame(
        {
            "source_id": ["s1", "s2"],
            "target_id": ["s2", "s1"],
            "commodity_category": ["working_bike", "working_bike"],
            "date": [date(2025, 1, 1), date(2025, 1, 2)],
            "quantity": [1.0, 2.0],
            "duration_hours": pd.array([None, None], dtype="Float64"),
            "modal_type": [None, None],
            "resource_id": [None, None],
        }
    )
    df["duration_hours"] = df["duration_hours"].astype("float64")
    parquet_path = tmp_path / "observed_flow.parquet"
    df.to_parquet(parquet_path)
    loaded = pd.read_parquet(parquet_path)
    assert loaded["duration_hours"].dtype.kind == "f"
    assert loaded["duration_hours"].isna().all()


def test_observed_flow_parquet_mixed_null_float_dtype(tmp_path: Path) -> None:
    """Mixed null/non-null ``duration_hours`` round-trips as float64."""
    df = pd.DataFrame(
        {
            "source_id": ["s1", "s2"],
            "target_id": ["s2", "s1"],
            "commodity_category": ["working_bike", "working_bike"],
            "date": [date(2025, 1, 1), date(2025, 1, 2)],
            "quantity": [1.0, 2.0],
            "duration_hours": [0.5, None],
            "modal_type": [None, None],
            "resource_id": [None, None],
        }
    )
    df["duration_hours"] = df["duration_hours"].astype("float64")
    parquet_path = tmp_path / "observed_flow.parquet"
    df.to_parquet(parquet_path)
    loaded = pd.read_parquet(parquet_path)
    assert loaded["duration_hours"].dtype.kind == "f"
    assert loaded["duration_hours"].iloc[0] == 0.5
    assert pd.isna(loaded["duration_hours"].iloc[1])
