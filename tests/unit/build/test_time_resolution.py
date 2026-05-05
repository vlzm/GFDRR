"""Tests for time resolution."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

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


def test_resolve_to_periods_per_column_mapping() -> None:
    """Per-column dict aggregator applies different funcs to different columns."""
    periods = pd.DataFrame(
        {
            "period_id": ["p0"],
            "start_date": [date(2025, 1, 1)],
            "end_date": [date(2025, 1, 3)],
        }
    )
    df = pd.DataFrame(
        {
            "source_id": ["s1", "s1"],
            "target_id": ["s2", "s2"],
            "commodity_category": ["working_bike", "working_bike"],
            "date": [date(2025, 1, 1), date(2025, 1, 2)],
            "quantity": [3.0, 5.0],
            "duration_hours": [0.2, 0.4],
        }
    )
    out = resolve_to_periods(
        df,
        periods,
        value_columns=["quantity", "duration_hours"],
        group_grain=["source_id", "target_id", "commodity_category"],
        agg_func={"quantity": "sum", "duration_hours": "mean"},
    )
    assert len(out) == 1
    row = out.iloc[0]
    assert row["quantity"] == 8.0
    assert row["duration_hours"] == pytest.approx(0.3)


def test_resolve_all_time_varying_includes_duration_hours_when_present() -> None:
    """Build pipeline carries duration_hours through observed_flow resolution."""
    raw = minimal_raw_model(with_observations=True)
    obs = raw.observed_flow.copy()
    obs["duration_hours"] = 0.25
    raw = raw.__class__(**{**raw.__dict__, "observed_flow": obs})
    periods = raw.periods
    resolved = resolve_all_time_varying(raw, periods)
    assert "observed_flow" in resolved
    assert "duration_hours" in resolved["observed_flow"].columns
    assert (resolved["observed_flow"]["duration_hours"] == 0.25).all()


def test_resolve_all_time_varying_observed_flow_without_duration() -> None:
    """Legacy observed_flow without duration_hours stays resolvable."""
    raw = minimal_raw_model(with_observations=True)
    periods = raw.periods
    resolved = resolve_all_time_varying(raw, periods)
    assert "observed_flow" in resolved
    assert "duration_hours" not in resolved["observed_flow"].columns
    assert "quantity" in resolved["observed_flow"].columns


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
