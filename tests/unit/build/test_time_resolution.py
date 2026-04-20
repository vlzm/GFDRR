"""Tests for time resolution."""

from __future__ import annotations

from datetime import date

import pandas as pd

from gbp.build.time_resolution import resolve_all_time_varying, resolve_to_periods
from tests.unit.build.fixtures import minimal_raw_model


def test_resolve_to_periods_sums_demand_into_periods() -> None:
    """Two dates in same period aggregate with sum."""
    periods = pd.DataFrame(
        {
            "period_id": ["p0"],
            "start_date": [date(2025, 1, 1)],
            "end_date": [date(2025, 1, 3)],
        }
    )
    df = pd.DataFrame(
        {
            "facility_id": ["s1", "s1"],
            "commodity_category": ["working_bike", "working_bike"],
            "date": [date(2025, 1, 1), date(2025, 1, 2)],
            "quantity": [1.0, 2.0],
        }
    )
    out = resolve_to_periods(
        df,
        periods,
        value_columns=["quantity"],
        group_grain=["facility_id", "commodity_category"],
        agg_func="sum",
    )
    assert len(out) == 1
    assert out.iloc[0]["period_id"] == "p0"
    assert out.iloc[0]["quantity"] == 3.0


def test_resolve_all_time_varying_on_minimal_raw() -> None:
    """Demand table picks up period_id and loses raw date column."""
    raw = minimal_raw_model()
    periods = raw.periods
    resolved = resolve_all_time_varying(raw, periods)
    assert "demand" in resolved
    d = resolved["demand"]
    assert "period_id" in d.columns
    assert "date" not in d.columns


def test_resolve_all_includes_observations() -> None:
    """Observed tables are resolved to period grain when present."""
    raw = minimal_raw_model(with_observations=True)
    periods = raw.periods
    resolved = resolve_all_time_varying(raw, periods)
    assert "observed_flow" in resolved
    assert "observed_inventory" in resolved
    assert "period_id" in resolved["observed_flow"].columns
    assert "date" not in resolved["observed_flow"].columns
    assert "period_id" in resolved["observed_inventory"].columns
    assert "date" not in resolved["observed_inventory"].columns


def test_empty_input_returns_empty_frame() -> None:
    """Empty param DataFrame yields empty resolved frame."""
    periods = pd.DataFrame(
        {
            "period_id": ["p0"],
            "start_date": [date(2025, 1, 1)],
            "end_date": [date(2025, 1, 2)],
        }
    )
    empty = pd.DataFrame(columns=["facility_id", "commodity_category", "date", "quantity"])
    out = resolve_to_periods(
        empty,
        periods,
        ["quantity"],
        ["facility_id", "commodity_category"],
        "sum",
    )
    assert out.empty
