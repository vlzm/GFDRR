"""Tests for the framework half of the forecast path.

The demand-table helpers (the counts grid, the one rounding rule), the
forecast artifact reader, and the hour-of-week OD mapping. The builders that
produce a forecast live in the domain and are tested in
``tests/test_ml_forecast.py``.
"""

import pandas as pd
import pytest

from gbp.ml import artifact
from gbp.model.dataloader_graph import (
    get_forecast_periods_df,
    hour_of_week,
    map_od_matrix_by_hour_of_week,
)

CLASSIC = "classic_bike"

#: 2026-01-05 is a Monday, so hour_of_week(t0 + h hours) == h for a grid
#: starting here — the arithmetic in the tests below stays readable.
MONDAY = pd.Timestamp("2026-01-05")


def hourly_periods(t0: pd.Timestamp, n: int) -> pd.DataFrame:
    return get_forecast_periods_df(t0, n, pd.Timedelta(hours=1))


def demand_table(rows: list[tuple[int, str, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "period_id": [r[0] for r in rows],
            "facility_id": [r[1] for r in rows],
            "commodity_category": CLASSIC,
            "quantity": [r[2] for r in rows],
        }
    )


# ---------------------------------------------------------------------------
# hour_of_week and the forecast period grid
# ---------------------------------------------------------------------------
def test_hour_of_week_counts_from_monday_midnight():
    ts = pd.Series(
        [
            MONDAY,  # Monday 00:00
            MONDAY + pd.Timedelta(hours=8),  # Monday 08:00
            MONDAY + pd.Timedelta(days=6, hours=23),  # Sunday 23:00
            MONDAY + pd.Timedelta(days=7),  # next Monday 00:00
        ]
    )
    assert hour_of_week(ts).tolist() == [0, 8, 167, 0]


def test_forecast_periods_restart_at_zero():
    grid = hourly_periods(MONDAY, 3)
    assert grid["period_id"].tolist() == [0, 1, 2]
    assert grid["start_timestamp"].iloc[0] == MONDAY
    assert grid["end_timestamp"].iloc[2] == MONDAY + pd.Timedelta(hours=3)


# ---------------------------------------------------------------------------
# The counts grid
# ---------------------------------------------------------------------------
def test_counts_from_demand_rejects_demand_outside_the_grid():
    periods = hourly_periods(MONDAY, 24)
    history = demand_table([(100, "s1", 1)])
    with pytest.raises(ValueError, match="outside the period grid"):
        artifact.counts_from_demand(history, periods)


def test_counts_from_demand_fills_the_grid_with_zeros():
    periods = hourly_periods(MONDAY, 24)
    counts = artifact.counts_from_demand(demand_table([(8, "s1", 2)]), periods)
    assert len(counts) == 24
    assert counts["quantity"].sum() == 2
    assert counts.loc[counts["quantity"] > 0, "start_timestamp"].tolist() == [
        MONDAY + pd.Timedelta(hours=8)
    ]


# ---------------------------------------------------------------------------
# The rounding rule
# ---------------------------------------------------------------------------
def test_round_forecast_demand_preserves_period_commodity_totals():
    fractional = demand_table(
        [(0, "s1", 0.4), (0, "s2", 0.4), (0, "s3", 0.4), (0, "s4", 0.4), (0, "s5", 0.4)]
    )
    out = artifact.round_forecast_demand(fractional)
    # Row-by-row rounding would give 0; the group total 2.0 survives.
    assert out["quantity"].sum() == 2
    assert (out["quantity"] >= 1).all()


def test_round_forecast_demand_never_invents_demand():
    fractional = demand_table([(0, "s1", 2.0), (0, "s2", 0.0), (0, "s3", 0.9)])
    out = artifact.round_forecast_demand(fractional)
    # s2 has fractional part zero, so it can never be rounded up; zero rows
    # are dropped from the table.
    assert set(out["facility_id"]) == {"s1", "s3"}
    assert out.set_index("facility_id")["quantity"].to_dict() == {"s1": 2, "s3": 1}


def test_round_forecast_demand_breaks_ties_by_facility_id():
    fractional = demand_table([(0, "s2", 0.5), (0, "s1", 0.5)])
    out = artifact.round_forecast_demand(fractional)
    # Total 1.0: one bike to hand out, equal remainders — s1 wins the tie.
    assert out["facility_id"].tolist() == ["s1"]
    assert out["quantity"].tolist() == [1]


# ---------------------------------------------------------------------------
# The forecast artifact
# ---------------------------------------------------------------------------
def test_load_forecast_unknown_name(tmp_path):
    with pytest.raises(FileNotFoundError, match="unknown forecast"):
        artifact.load_forecast("missing", tmp_path)


# ---------------------------------------------------------------------------
# OD mapping by hour of week
# ---------------------------------------------------------------------------
def test_map_od_matrix_pools_periods_with_one_hour_of_week():
    periods = hourly_periods(MONDAY, 2 * 168)
    # Monday 08:00 in two different weeks: 3 trips s1->s2 (1 period long) and
    # 1 trip s1->s3 (2 periods long) in week one; 2 trips s1->s3 in week two.
    od = pd.DataFrame(
        {
            "source_id": ["s1", "s1", "s1"],
            "planned_target_id": ["s2", "s3", "s3"],
            "period_id": [8, 8, 168 + 8],
            "commodity_category": CLASSIC,
            "count": [3, 1, 2],
            "duration": [1, 2, 5],
            "probability": [0.75, 0.25, 1.0],
        }
    )
    horizon = hourly_periods(MONDAY + pd.Timedelta(days=14), 168)

    out = map_od_matrix_by_hour_of_week(od, periods, horizon)

    # Every mapped row lands on the forecast period with the same hour of week.
    assert set(out["period_id"]) == {8}
    row = out.set_index("planned_target_id")
    # Counts pool across the weeks; the shares are recomputed from the pool.
    assert row.loc["s2", "count"] == 3
    assert row.loc["s3", "count"] == 3
    assert row.loc["s2", "probability"] == pytest.approx(0.5)
    assert row.loc["s3", "probability"] == pytest.approx(0.5)
    # Duration is the count-weighted mean: (1*2 + 2*5) / 3 = 4.
    assert row.loc["s3", "duration"] == 4
