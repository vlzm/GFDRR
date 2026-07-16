"""Tests for the forecast-vs-actual metrics in ``gbp/ml/metrics.py``."""

import pandas as pd
import pytest

from gbp.ml.metrics import busy_facility_ids, lost_demand_busy_share, panel_departed_mae

CLASSIC = "classic_bike"


def _panel(rows: list[tuple[str, int, int, int]]) -> pd.DataFrame:
    """Build a run panel from (facility_id, period_id, departed, lost_demand) rows."""
    return pd.DataFrame(
        [
            {"facility_id": f, "period_id": p, "departed": d, "lost_demand": lost}
            for f, p, d, lost in rows
        ]
    )


def _demand(rows: list[tuple[int, str, int]]) -> pd.DataFrame:
    """Build a demand table from (period_id, facility_id, quantity) rows."""
    return pd.DataFrame(
        [
            {"period_id": p, "facility_id": f, "commodity_category": CLASSIC, "quantity": q}
            for p, f, q in rows
        ]
    )


def test_panel_departed_mae_averages_the_per_station_hour_gap():
    reference_panel = _panel([("s1", 0, 8, 0), ("s2", 0, 2, 0)])
    forecast_panel = _panel([("s1", 0, 6, 2), ("s2", 0, 2, 1)])
    # |6-8| at s1, |2-2| at s2, mean 1.0.
    assert panel_departed_mae(forecast_panel, reference_panel) == pytest.approx(1.0)


def test_panel_departed_mae_treats_a_missing_station_as_zero():
    reference_panel = _panel([("s1", 0, 4, 0)])
    forecast_panel = _panel([("s1", 0, 4, 0), ("s2", 0, 3, 0)])
    # s2 has no reference row: |3-0| = 3, s1 matches, mean 1.5.
    assert panel_departed_mae(forecast_panel, reference_panel) == pytest.approx(1.5)


def test_lost_demand_busy_share_splits_by_the_busy_stations():
    # s1 makes 8 of the 10 actual departures, so it is the one busy station.
    actual_df = _demand([(0, "s1", 8), (0, "s2", 2)])
    busy = busy_facility_ids(actual_df)
    panel = _panel([("s1", 0, 6, 2), ("s2", 0, 2, 1)])
    # lost 2 at the busy s1, 1 at s2, total 3 -> busy share 2/3.
    assert lost_demand_busy_share(panel, busy) == pytest.approx(2 / 3)


def test_lost_demand_busy_share_is_zero_when_nothing_is_lost():
    panel = _panel([("s1", 0, 5, 0)])
    assert lost_demand_busy_share(panel, {"s1"}) == 0.0
