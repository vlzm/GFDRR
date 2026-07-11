"""Tests for the two-level evaluation module (``app/evaluate.py``).

The heavy path (resolve the scenario, run 744 periods) is exercised by the
evaluation itself; these tests pin the one piece of logic the runs depend on:
the demand cut that makes every run face the same universe.
"""

import pathlib
import sys
import types

import pandas as pd
import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "app"))

import evaluate  # noqa: E402  (needs the app folder on sys.path)

CLASSIC = "classic_bike"


def _hourly_periods(start: str, n_periods: int) -> pd.DataFrame:
    periods = pd.DataFrame({"period_id": range(n_periods)})
    periods["start_timestamp"] = pd.Timestamp(start) + periods["period_id"] * pd.Timedelta(hours=1)
    periods["end_timestamp"] = periods["start_timestamp"] + pd.Timedelta(hours=1)
    return periods


def _graph_data() -> types.SimpleNamespace:
    """Build a scenario slice with OD coverage only for s1 at Monday 00:00."""
    graph = types.SimpleNamespace()
    # Two scenario periods on Monday 2026-01-05: hours of week 0 and 1.
    graph.periods_df = _hourly_periods("2026-01-05", 2)
    graph.facilities_df = pd.DataFrame({"facility_id": ["s1", "s2"]})
    graph.historical_od_matrix_df = pd.DataFrame(
        {
            "source_id": ["s1"],
            "planned_target_id": ["s2"],
            "period_id": [0],
            "commodity_category": [CLASSIC],
            "quantity": [1],
            "probability": [1.0],
            "mean_duration_periods": [1.0],
        }
    )
    return graph


def test_restrict_demand_keeps_only_covered_station_hours():
    graph = _graph_data()
    # The demand grid is the next Monday, so its hours of week line up with
    # the scenario's: period 0 -> hour of week 0 (covered for s1), period 1 ->
    # hour of week 1 (not covered).
    periods_df = _hourly_periods("2026-01-12", 2)
    demand_df = pd.DataFrame(
        {
            "period_id": [0, 1, 0],
            "facility_id": ["s1", "s1", "s2"],
            "commodity_category": [CLASSIC] * 3,
            "quantity": [3, 2, 4],
        }
    )

    kept, dropped_share = evaluate.restrict_demand_to_scenario(demand_df, graph, periods_df)

    # s1 at the covered hour stays; s1 at the uncovered hour and the unknown
    # station s2 are dropped: 6 of 9 bikes.
    assert kept.to_dict("records") == [
        {"period_id": 0, "facility_id": "s1", "commodity_category": CLASSIC, "quantity": 3}
    ]
    assert dropped_share == pytest.approx(6 / 9)


def test_restrict_demand_passes_a_fully_covered_table_through():
    graph = _graph_data()
    periods_df = _hourly_periods("2026-01-12", 1)
    demand_df = pd.DataFrame(
        {
            "period_id": [0],
            "facility_id": ["s1"],
            "commodity_category": [CLASSIC],
            "quantity": [5],
        }
    )
    kept, dropped_share = evaluate.restrict_demand_to_scenario(demand_df, graph, periods_df)
    pd.testing.assert_frame_equal(kept, demand_df)
    assert dropped_share == 0.0
