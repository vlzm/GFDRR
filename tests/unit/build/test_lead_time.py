"""Tests for lead time resolution."""

from __future__ import annotations

from datetime import date

import pandas as pd

from gbp.build.lead_time import resolve_lead_times
from gbp.core.enums import PeriodType


def test_uniform_day_lead_time_one_period() -> None:
    """24h lead with DAY periods yields one period offset."""
    edges = pd.DataFrame(
        {
            "source_id": ["a"],
            "target_id": ["b"],
            "modal_type": ["road"],
            "lead_time_hours": [24.0],
        }
    )
    periods = pd.DataFrame(
        {
            "period_id": ["p0", "p1"],
            "start_date": [date(2025, 1, 1), date(2025, 1, 2)],
            "end_date": [date(2025, 1, 2), date(2025, 1, 3)],
            "period_index": [0, 1],
            "period_type": [PeriodType.DAY.value, PeriodType.DAY.value],
        }
    )
    out = resolve_lead_times(edges, periods)
    row_p0 = out[out["period_id"] == "p0"].iloc[0]
    assert row_p0["lead_time_periods"] == 1
    assert row_p0["arrival_period_id"] == "p1"


def test_beyond_horizon_arrival_null() -> None:
    """Last departure period may have no arrival within horizon."""
    edges = pd.DataFrame(
        {
            "source_id": ["a"],
            "target_id": ["b"],
            "modal_type": ["road"],
            "lead_time_hours": [72.0],
        }
    )
    periods = pd.DataFrame(
        {
            "period_id": ["p0", "p1"],
            "start_date": [date(2025, 1, 1), date(2025, 1, 2)],
            "end_date": [date(2025, 1, 2), date(2025, 1, 3)],
            "period_index": [0, 1],
            "period_type": [PeriodType.DAY.value, PeriodType.DAY.value],
        }
    )
    out = resolve_lead_times(edges, periods)
    row_p1 = out[out["period_id"] == "p1"].iloc[0]
    assert pd.isna(row_p1["arrival_period_id"])
