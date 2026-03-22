"""Tests for dict/JSON serialization of model data."""

from __future__ import annotations

from datetime import date

import pandas as pd

from gbp.build.pipeline import build_model
from gbp.io.dict_io import (
    raw_from_dict,
    raw_to_dict,
    resolved_from_dict,
    resolved_to_dict,
)
from tests.unit.build.fixtures import minimal_raw_model


def test_raw_dict_round_trip() -> None:
    """``raw_from_dict(raw_to_dict(raw))`` yields equal DataFrames."""
    raw = minimal_raw_model(with_edges=True, with_demand=True)
    data = raw_to_dict(raw)
    loaded = raw_from_dict(data)

    for name in ("facilities", "periods", "edge_rules"):
        pd.testing.assert_frame_equal(
            getattr(raw, name),
            getattr(loaded, name),
            check_dtype=False,
        )


def test_resolved_dict_round_trip() -> None:
    """``resolved_from_dict(resolved_to_dict(r))`` round-trips."""
    raw = minimal_raw_model(with_edges=True, with_demand=True)
    resolved = build_model(raw)
    data = resolved_to_dict(resolved)
    loaded = resolved_from_dict(data)

    pd.testing.assert_frame_equal(
        resolved.facilities,
        loaded.facilities,
        check_dtype=False,
    )


def test_date_serialization() -> None:
    """Date columns survive round-trip as datetime.date objects."""
    raw = minimal_raw_model(with_edges=True, with_demand=True)
    data = raw_to_dict(raw)
    loaded = raw_from_dict(data)

    assert loaded.demand is not None
    assert loaded.demand["date"].iloc[0] == date(2025, 1, 1)
