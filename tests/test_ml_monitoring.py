"""Tests for monitoring (phase 7 of the demand forecasting plan).

The metrics step: saved forecasts scored against an actual month's counts,
one idempotent row per (forecast, month) in the metrics table, the naive
baseline next to each row. The alert rule: a month is degraded when the
rolling MAE is worse than the baseline. The drift report: the month's
feature distributions against the champion's training months, summarized
into a JSON file next to the HTML report.
"""

import json
import pathlib
import types

import pandas as pd
import pytest

from domains.citybike.loaders.download import month_bounds
from domains.citybike.ml.data import MlPaths
from domains.citybike.ml.ops import monitoring
from gbp.ml.artifact import ForecastMeta, save_forecast

CLASSIC = "classic_bike"
FACILITIES = ["s1", "s2"]


def partition_frame(month: str, quantity: int, temperature: float = 10.0) -> pd.DataFrame:
    """One month's training partition: every hour × two stations, one count each.

    The feature columns are synthetic (modulo patterns over the hour number),
    varied enough that the drift distances are well defined; ``temperature``
    shifts the two temperature columns, which is how a test forces drift.
    """
    start, end = month_bounds(month)
    n = int((end - start) / pd.Timedelta(hours=1))
    hours = pd.DataFrame(
        {
            "period_id": pd.Series(range(n), dtype="int64"),
            "start_timestamp": start + pd.to_timedelta(range(n), unit="h"),
        }
    )
    table = hours.merge(pd.DataFrame({"facility_id": FACILITIES}), how="cross")
    table["commodity_category"] = CLASSIC
    table["quantity"] = pd.Series(quantity, index=table.index, dtype="int64")
    table["hour_of_day"] = (table["period_id"] % 24).astype("int64")
    table["day_of_week"] = ((table["period_id"] // 24) % 7).astype("int64")
    table["month"] = ((table["period_id"] % 12) + 1).astype("int64")
    table["is_holiday"] = table["period_id"] % 30 == 0
    table["temperature_max_c"] = temperature + (table["period_id"] % 7) * 0.5
    table["temperature_min_c"] = temperature - 5.0 + (table["period_id"] % 5) * 0.5
    table["precipitation_mm"] = (table["period_id"] % 3).astype("float64")
    table["quantity_lag_1w"] = quantity + (table["period_id"] % 4) * 0.25
    table["quantity_mean_4w"] = quantity + (table["period_id"] % 5) * 0.25
    table["facility_mean"] = quantity + (table["period_id"] % 6) * 0.25
    table["facility_hour_of_week_mean"] = quantity + (table["period_id"] % 7) * 0.25
    return table


def save_artifact(
    root: pathlib.Path,
    name: str,
    t0: pd.Timestamp,
    horizon_periods: int,
    quantity: int,
    model_version: str = "7",
) -> None:
    """Save a small forecast artifact: ``quantity`` bikes per hour and station."""
    grid = pd.DataFrame({"period_id": pd.Series(range(horizon_periods), dtype="int64")})
    demand = grid.merge(pd.DataFrame({"facility_id": FACILITIES}), how="cross")
    demand["commodity_category"] = CLASSIC
    demand["quantity"] = pd.Series(quantity, index=demand.index, dtype="int64")
    meta = ForecastMeta(
        forecast_name=name,
        model_name="lightgbm",
        model_version=model_version,
        created_at="2026-07-11T00:00:00",
        t0=t0.isoformat(),
        horizon_periods=horizon_periods,
        period_len_hours=1.0,
        history_start="2025-12-01T00:00:00",
        history_end=t0.isoformat(),
        inputs=[],
    )
    save_forecast(demand, meta, root)


def forecast_meta(t0: str, horizon_periods: int, period_len_hours: float = 1.0) -> ForecastMeta:
    return ForecastMeta(
        forecast_name="f",
        model_name="lightgbm",
        model_version="1",
        created_at="2026-07-11T00:00:00",
        t0=t0,
        horizon_periods=horizon_periods,
        period_len_hours=period_len_hours,
        history_start="2025-12-01T00:00:00",
        history_end=t0,
        inputs=[],
    )


# ---------------------------------------------------------------------------
# Lining a forecast horizon up with a month
# ---------------------------------------------------------------------------
def test_month_period_map_lines_up_a_horizon_crossing_the_month_edge():
    # 5 hours from 2026-01-31 22:00: two land in January, three in February.
    meta = forecast_meta("2026-01-31T22:00:00", 5)

    january = monitoring.month_period_map(meta, "202601")
    assert january["period_id"].tolist() == [0, 1]
    assert january["month_period_id"].tolist() == [742, 743]  # the month's last two hours

    february = monitoring.month_period_map(meta, "202602")
    assert february["period_id"].tolist() == [2, 3, 4]
    assert february["month_period_id"].tolist() == [0, 1, 2]

    assert monitoring.month_period_map(meta, "202603").empty


def test_month_period_map_rejects_periods_that_are_not_the_months_hours():
    # A 2-hour period grid cannot be lined up with the hourly partitions.
    assert monitoring.month_period_map(forecast_meta("2026-01-01T00:00:00", 5, 2.0), "202601").empty
    # An on-the-half-hour start falls between the month's hours.
    assert monitoring.month_period_map(forecast_meta("2026-01-01T00:30:00", 5), "202601").empty


# ---------------------------------------------------------------------------
# Scoring a month
# ---------------------------------------------------------------------------
@pytest.fixture
def roots(tmp_path):
    paths = MlPaths.under(tmp_path)
    paths.training.mkdir()
    return paths


def test_score_month_scores_overlapping_forecasts_and_the_baseline(roots):
    # History: 2 departures every hour -> the naive predicts 2 for January.
    # Actual January: 3 every hour. Forecast: 4 every hour for one day.
    partition_frame("202512", 2).to_parquet(roots.training / "202512.parquet")
    partition_frame("202601", 3).to_parquet(roots.training / "202601.parquet")
    save_artifact(roots.forecasts, "jan_day1", pd.Timestamp("2026-01-01"), 24, 4)
    save_artifact(roots.forecasts, "march", pd.Timestamp("2026-03-01"), 24, 4)

    rows = monitoring.score_month(
        "202601",
        paths=roots,
        log=lambda message: None,
    )

    assert rows["forecast_name"].tolist() == ["jan_day1"]  # March does not touch January
    row = rows.iloc[0]
    assert row["model_name"] == "lightgbm"
    assert row["model_version"] == "7"
    assert row["n_periods"] == 24
    assert row["mae"] == 1.0  # |4 - 3|, only over the day the forecast covers
    assert row["bias"] == 1.0  # the forecast overpredicts
    assert row["naive_mae"] == 1.0  # |2 - 3|
    assert row["poisson_deviance"] > 0


def test_score_month_replaces_rows_instead_of_duplicating_them(roots):
    partition_frame("202512", 2).to_parquet(roots.training / "202512.parquet")
    partition_frame("202601", 3).to_parquet(roots.training / "202601.parquet")
    save_artifact(roots.forecasts, "jan_day1", pd.Timestamp("2026-01-01"), 24, 4)

    for _ in range(2):
        monitoring.score_month(
            "202601",
            paths=roots,
            log=lambda message: None,
        )

    table = monitoring.load_metrics(roots.monitoring)
    assert len(table) == 1


def test_score_month_without_history_leaves_the_baseline_missing(roots):
    partition_frame("202601", 3).to_parquet(roots.training / "202601.parquet")
    save_artifact(roots.forecasts, "jan_day1", pd.Timestamp("2026-01-01"), 24, 4)

    rows = monitoring.score_month(
        "202601",
        paths=roots,
        log=lambda message: None,
    )

    assert rows.iloc[0]["mae"] == 1.0
    assert pd.isna(rows.iloc[0]["naive_mae"])


# ---------------------------------------------------------------------------
# The alert rule
# ---------------------------------------------------------------------------
def metrics_rows(maes: list[float], naive: float = 2.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "month": [f"20260{i + 1}" for i in range(len(maes))],
            "forecast_name": [f"f{i}" for i in range(len(maes))],
            "model_name": "lightgbm",
            "model_version": "1",
            "n_periods": 24,
            "mae": maes,
            "poisson_deviance": 0.5,
            "bias": 0.1,
            "naive_mae": naive,
            "scored_at": "2026-07-11T00:00:00",
        }
    )


def test_metric_history_marks_the_month_where_rolling_mae_crosses_the_baseline():
    # Rolling MAE over 3 months: 1.0, 1.0, 7/3. The baseline is 2.0, so only
    # the third month is degraded.
    history = monitoring.metric_history(metrics_rows([1.0, 1.0, 5.0]))
    assert history["rolling_mae"].tolist() == [1.0, 1.0, pytest.approx(7 / 3)]
    assert history["degraded"].tolist() == [False, False, True]


def test_metric_history_never_marks_a_month_without_a_baseline():
    history = monitoring.metric_history(metrics_rows([5.0, 5.0], naive=float("nan")))
    assert history["degraded"].tolist() == [False, False]


def test_metric_history_averages_forecasts_of_the_same_version_and_month():
    rows = pd.concat([metrics_rows([1.0]), metrics_rows([3.0])], ignore_index=True)
    history = monitoring.metric_history(rows)
    assert len(history) == 1
    assert history.iloc[0]["mae"] == 2.0


def test_metric_history_on_an_empty_table():
    assert monitoring.metric_history(monitoring.load_metrics(pathlib.Path("/nonexistent"))).empty


# ---------------------------------------------------------------------------
# The drift report
# ---------------------------------------------------------------------------
def test_drift_report_compares_the_month_against_the_champions_training_data(roots, monkeypatch):
    import domains.citybike.ml.ops.registry as registry

    # December is the reference; January's temperatures are shifted far up.
    partition_frame("202512", 2, temperature=10.0).to_parquet(roots.training / "202512.parquet")
    partition_frame("202601", 2, temperature=40.0).to_parquet(roots.training / "202601.parquet")
    champion = types.SimpleNamespace(
        version="7", tags={"model_family": "lightgbm", "train_months": "202512,202601"}
    )
    monkeypatch.setattr(registry.MlflowStore, "champion_version", lambda self: champion)

    summary_path = monitoring.drift_report(
        "202601",
        paths=roots,
        sample_rows=500,
        log=lambda message: None,
    )

    summary = json.loads(summary_path.read_text())
    # The monitored month never sits on its own reference side.
    assert summary["reference_months"] == ["202512"]
    assert summary["champion_version"] == "7"
    assert summary["current_rows"] == 500
    columns = {c["column"]: c for c in summary["columns"]}
    # Only the weather and demand-history columns are checked; the calendar
    # columns (month above all) would drift by construction.
    assert set(columns) == set(monitoring.DRIFT_COLUMNS)
    assert "month" not in columns
    assert columns["temperature_max_c"]["drifted"] is True
    assert columns["temperature_max_c"]["threshold"] == monitoring.DRIFT_NUM_THRESHOLD
    assert columns["precipitation_mm"]["drifted"] is False
    assert summary["drifted_count"] >= 2  # both temperature columns moved
    html_path, _ = monitoring.drift_report_paths("202601", roots.monitoring)
    assert html_path.exists()
    assert monitoring.list_drift_summaries(roots.monitoring)[0]["month"] == "202601"


def test_drift_report_without_a_champion_raises(roots, monkeypatch):
    import domains.citybike.ml.ops.registry as registry

    monkeypatch.setattr(registry.MlflowStore, "champion_version", lambda self: None)
    with pytest.raises(LookupError, match="no champion"):
        monitoring.drift_report("202601", paths=roots)
