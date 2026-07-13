"""Tests for phase 4: the model interface, the families, the metrics, the backtest.

Every model family runs on one tiny fixture — two stations with a fixed
weekly pattern — so the expected numbers are exact. The backtest test writes
three tiny training partitions and drives the whole loop, MLflow store
included.
"""

import numpy as np
import pandas as pd
import pytest

from gbp.loaders.download import month_bounds
from gbp.ml import features
from gbp.ml.backtest import (
    EXPERIMENT_NAME,
    backtest_splits,
    comparison_table,
    run_backtest,
)
from gbp.ml.metrics import (
    align_forecast,
    busy_facility_ids,
    forecast_metrics,
    mean_absolute_error,
    mean_poisson_deviance,
)
from gbp.ml.models import MODEL_FAMILIES, create_model
from gbp.ml.models.graph import GraphSageModel, station_graph_edges
from gbp.ml.registry import MlflowStore

CLASSIC = "classic_bike"

#: 2025-02-03 is a Monday, so hour-of-week arithmetic stays readable.
MONDAY = pd.Timestamp("2025-02-03")
WEEK = pd.Timedelta(weeks=1)


# ---------------------------------------------------------------------------
# Fixtures: a full grid with a fixed weekly pattern
# ---------------------------------------------------------------------------
def pattern_quantity(table: pd.DataFrame) -> pd.Series:
    """Give the fixture's demand law: s1 rides 2/hour (5 at 08:00), s2 rides 1/hour."""
    peak = (table["start_timestamp"].dt.hour == 8).astype("int64")
    return pd.Series(
        np.where(table["facility_id"] == "s1", 2 + 3 * peak, 1),
        index=table.index,
        dtype="int64",
    )


def grid_counts(hours: pd.DatetimeIndex) -> pd.DataFrame:
    """Full-grid departure counts for s1 and s2 over the given hours."""
    grid = pd.MultiIndex.from_product(
        [hours, ["s1", "s2"], [CLASSIC]],
        names=["start_timestamp", "facility_id", "commodity_category"],
    ).to_frame(index=False)
    grid["quantity"] = pattern_quantity(grid)
    return grid


def with_training_columns(counts: pd.DataFrame, period_start: pd.Timestamp) -> pd.DataFrame:
    """Add the feature columns and ``period_id`` to a counts grid.

    The weekly pattern repeats exactly, so the history features are the
    pattern itself — the same values the real feature builder would compute
    from enough past weeks.
    """
    table = features.add_calendar_features(counts)
    for column in features.WEATHER_FEATURES:
        table[column] = np.nan
    table[features.WEATHER_FEATURES] = table[features.WEATHER_FEATURES].astype("float64")
    law = pattern_quantity(table).astype("float64")
    table["quantity_lag_1w"] = law
    table["quantity_mean_4w"] = law
    table["facility_mean"] = table.groupby("facility_id", observed=True)["quantity"].transform(
        "mean"
    )
    table["facility_hour_of_week_mean"] = law
    table["stockout_share"] = np.full(len(table), np.nan, dtype="float64")
    table["period_id"] = (
        (table["start_timestamp"] - period_start) // pd.Timedelta(hours=1)
    ).astype("int64")
    return table


def tiny_training_table(weeks: int = 6, start: pd.Timestamp = MONDAY) -> pd.DataFrame:
    """Build a training table over ``weeks`` weeks of the fixed pattern."""
    hours = pd.date_range(start, periods=weeks * 168, freq="h")
    return with_training_columns(grid_counts(hours), start)


def tiny_forecast_input(start: pd.Timestamp) -> pd.DataFrame:
    """Build a one-week forecast input right after the training weeks."""
    hours = pd.date_range(start, periods=168, freq="h")
    table = with_training_columns(grid_counts(hours), start)
    return table.drop(columns=["quantity", "stockout_share"])


def tiny_edges() -> pd.DataFrame:
    """One edge between the two stations of the fixture."""
    return pd.DataFrame({"source_id": ["s1"], "target_id": ["s2"], "trips": [5]})


def make_model(name: str):
    """Build one small model of the family, sized for the tiny fixture."""
    if name == "lightgbm":
        return create_model(name, num_boost_round=50, min_data_in_leaf=5)
    if name == "graphsage":
        return create_model(
            name,
            edges_df=tiny_edges(),
            hidden_size=8,
            epochs=2,
            batch_size=32,
            train_window_months=2,
        )
    return create_model(name)


# ---------------------------------------------------------------------------
# The interface: every family accepts the same tables, returns the same shape
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("name", MODEL_FAMILIES)
def test_every_family_fits_and_predicts_the_fractional_demand_shape(name):
    model = make_model(name)
    training_table = tiny_training_table()
    horizon_start = MONDAY + 6 * WEEK
    model.fit(training_table)
    out = model.predict(tiny_forecast_input(horizon_start))

    assert list(out.columns) == ["period_id", "facility_id", "commodity_category", "quantity"]
    assert len(out) == 168 * 2  # the full horizon grid
    assert (out["quantity"] >= 0).all()
    assert out["quantity"].dtype == "float64"
    keys = ["period_id", "facility_id", "commodity_category"]
    assert out[keys].equals(out[keys].sort_values(keys).reset_index(drop=True))


@pytest.mark.parametrize("name", MODEL_FAMILIES)
def test_every_family_survives_a_save_load_round_trip(name, tmp_path):
    model = make_model(name)
    model.fit(tiny_training_table())
    horizon = tiny_forecast_input(MONDAY + 6 * WEEK)
    before = model.predict(horizon)

    model.save(tmp_path / name)
    reloaded = type(model).load(tmp_path / name)
    after = reloaded.predict(horizon)

    pd.testing.assert_frame_equal(before, after)


def test_create_model_rejects_an_unknown_family():
    with pytest.raises(ValueError, match="unknown model family"):
        create_model("prophet")


# ---------------------------------------------------------------------------
# Family-specific behavior
# ---------------------------------------------------------------------------
def test_seasonal_naive_predicts_the_hour_of_week_mean():
    model = make_model("seasonal_naive")
    model.fit(tiny_training_table())
    table = tiny_forecast_input(MONDAY + 6 * WEEK)
    out = model.predict(table)
    expected = table.sort_values(["period_id", "facility_id", "commodity_category"])
    assert out["quantity"].tolist() == expected["facility_hour_of_week_mean"].tolist()


def test_sarimax_forecasts_the_daily_total_and_splits_it():
    # The fixture's daily city total is constant: s1 gives 2*24+3=51, s2 gives 24.
    model = make_model("sarimax")
    model.fit(tiny_training_table())
    out = model.predict(tiny_forecast_input(MONDAY + 6 * WEEK))
    day_totals = out.groupby(out["period_id"] // 24)["quantity"].sum()
    assert day_totals.values == pytest.approx([75.0] * 7, rel=0.05)


def test_sarimax_refuses_to_backcast():
    model = make_model("sarimax")
    model.fit(tiny_training_table())
    with pytest.raises(ValueError, match="only forecasts forward"):
        model.predict(tiny_forecast_input(MONDAY + 2 * WEEK))


def test_lightgbm_learns_the_weekly_pattern():
    model = make_model("lightgbm")
    model.fit(tiny_training_table())
    table = tiny_forecast_input(MONDAY + 6 * WEEK)
    out = model.predict(table)
    truth = pattern_quantity(
        table.sort_values(["period_id", "facility_id", "commodity_category"]).reset_index(drop=True)
    )
    assert mean_absolute_error(truth.to_numpy("float64"), out["quantity"].to_numpy()) < 0.5


def test_graphsage_rejects_a_table_with_grid_holes():
    model = make_model("graphsage")
    table = tiny_training_table(weeks=2)
    with pytest.raises(ValueError, match="full grid"):
        model.fit(table.iloc[:-1])


def write_trip_csv(folder, month: str, pairs) -> None:
    """Write one raw trip CSV for ``month``: one ride per (start, end) station pair."""
    header = (
        "ride_id,rideable_type,started_at,ended_at,start_station_name,start_station_id,"
        "end_station_name,end_station_id,start_lat,start_lng,end_lat,end_lng,member_casual"
    )
    day = f"{month[:4]}-{month[4:]}-03"
    rows = [
        f"R{i},{CLASSIC},{day} 08:0{i}:00,{day} 08:2{i}:00,A,{s},B,{t},"
        "40.75,-73.99,40.76,-73.97,member"
        for i, (s, t) in enumerate(pairs)
    ]
    (folder / f"{month}-citibike-tripdata_1.csv").write_text("\n".join([header, *rows]) + "\n")


def test_station_graph_edges_counts_pairs_and_drops_self_loops(tmp_path):
    write_trip_csv(tmp_path, "202502", [("s1", "s2"), ("s1", "s2"), ("s2", "s1"), ("s1", "s1")])

    edges = station_graph_edges(["202502"], raw=tmp_path)

    by_pair = edges.set_index(["source_id", "target_id"])["trips"]
    assert by_pair[("s1", "s2")] == 2
    assert by_pair[("s2", "s1")] == 1
    assert ("s1", "s1") not in by_pair.index


def test_graphsage_counts_its_edges_in_fit_when_none_are_given(tmp_path):
    # The training table ends in March 2025, so fit reads the March raw file.
    write_trip_csv(tmp_path, "202503", [("s1", "s2"), ("s2", "s1")])
    model = create_model(
        "graphsage",
        hidden_size=8,
        epochs=1,
        batch_size=32,
        train_window_months=2,
        raw=tmp_path,
    )
    assert model.params()["n_edges"] == 0

    model.fit(tiny_training_table())

    assert model.params()["n_edges"] > 0
    out = model.predict(tiny_forecast_input(MONDAY + 6 * WEEK))
    assert len(out) == 168 * 2


def test_graphsage_fit_without_edges_needs_the_raw_files(tmp_path):
    model = create_model("graphsage", raw=tmp_path)  # an empty folder: no trip CSVs
    with pytest.raises(FileNotFoundError, match="no raw CSVs"):
        model.fit(tiny_training_table())


def test_graphsage_neighbor_cap_keeps_the_strongest_edges():
    edges = pd.DataFrame(
        {
            "source_id": ["a", "a", "a"],
            "target_id": ["b", "c", "d"],
            "trips": [5, 9, 1],
        }
    )
    model = GraphSageModel(edges, max_neighbors=2)
    kept = model._edges[model._edges["source_id"] == "a"]
    assert sorted(kept["target_id"]) == ["b", "c"]


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
def demand_rows(rows):
    """Build a demand-shaped table from ``(period_id, facility_id, quantity)`` tuples."""
    return pd.DataFrame(
        {
            "period_id": [r[0] for r in rows],
            "facility_id": [r[1] for r in rows],
            "commodity_category": CLASSIC,
            "quantity": [r[2] for r in rows],
        }
    )


def test_align_forecast_outer_joins_and_fills_zero():
    actual = demand_rows([(0, "s1", 3)])
    predicted = demand_rows([(0, "s2", 2.0)])
    aligned = align_forecast(actual, predicted).set_index("facility_id")
    assert aligned.loc["s1", "actual"] == 3.0 and aligned.loc["s1", "predicted"] == 0.0
    assert aligned.loc["s2", "actual"] == 0.0 and aligned.loc["s2", "predicted"] == 2.0


def test_poisson_deviance_is_zero_for_a_perfect_forecast():
    # A zero row contributes 2 * POISSON_EPS (its prediction is floored),
    # so the mean is tiny but not exactly zero.
    values = np.array([0.0, 1.0, 5.0])
    assert mean_poisson_deviance(values, values) == pytest.approx(0.0, abs=1e-5)


def test_poisson_deviance_of_a_zero_actual_is_twice_the_prediction():
    assert mean_poisson_deviance(np.array([0.0]), np.array([2.0])) == pytest.approx(4.0)


def test_busy_facilities_cover_half_of_the_departures():
    actual = demand_rows([(0, "a", 10), (0, "b", 6), (0, "c", 1)])
    # Half of 17 is 8.5; "a" alone crosses it.
    assert busy_facility_ids(actual) == {"a"}


def test_forecast_metrics_split_busy_and_quiet():
    actual = demand_rows([(0, "a", 10), (0, "b", 2)])
    predicted = demand_rows([(0, "a", 8.0), (0, "b", 2.0)])
    metrics = forecast_metrics(actual, predicted)
    assert metrics["mae"] == pytest.approx(1.0)  # (2 + 0) / 2
    assert metrics["mae_busy"] == pytest.approx(2.0)  # station "a" only
    assert metrics["mae_quiet"] == pytest.approx(0.0)
    assert metrics["actual_total"] == 12.0
    assert metrics["predicted_total"] == 10.0


# ---------------------------------------------------------------------------
# Backtest splits and the comparison table
# ---------------------------------------------------------------------------
def test_backtest_splits_roll_the_origin_forward():
    splits = backtest_splits(["202502", "202503", "202504", "202505"], n_splits=2)
    assert [(s.train_months, s.test_month) for s in splits] == [
        (("202502", "202503"), "202504"),
        (("202502", "202503", "202504"), "202505"),
    ]


def test_backtest_splits_reject_a_gap():
    with pytest.raises(ValueError, match="not consecutive"):
        backtest_splits(["202502", "202504", "202505"], n_splits=1)


def test_backtest_splits_need_a_training_month():
    with pytest.raises(ValueError, match="cannot give"):
        backtest_splits(["202502", "202503"], n_splits=2)


def test_comparison_table_relates_models_to_the_baseline():
    records = [
        {"model": "seasonal_naive", "test_month": "202504", "mae": 2.0, "poisson_deviance": 4.0},
        {"model": "lightgbm", "test_month": "202504", "mae": 1.0, "poisson_deviance": 2.0},
    ]
    baseline = [{"test_month": "202504", "mae": 2.0, "poisson_deviance": 4.0}]
    table = comparison_table(records, baseline).set_index("model")
    assert table.loc["lightgbm", "mae_over_naive"] == pytest.approx(0.5)
    assert table.loc["seasonal_naive", "poisson_deviance_over_naive"] == pytest.approx(1.0)


def test_comparison_table_without_a_baseline_has_no_ratio_columns():
    records = [{"model": "lightgbm", "test_month": "202504", "mae": 1.0, "poisson_deviance": 2.0}]
    assert "mae_over_naive" not in comparison_table(records).columns


# ---------------------------------------------------------------------------
# The backtest end to end, MLflow store included
# ---------------------------------------------------------------------------
def month_partition(month: str) -> pd.DataFrame:
    """One tiny training partition: the fixture pattern over a real month."""
    start, end = month_bounds(month)
    hours = pd.date_range(start, end - pd.Timedelta(hours=1), freq="h")
    return with_training_columns(grid_counts(hours), start)


def flat_weather(start: str, end: str) -> pd.DataFrame:
    """Constant daily weather over ``[start, end]``."""
    dates = pd.date_range(start, end, freq="D")
    return pd.DataFrame(
        {
            "date": dates,
            "temperature_max_c": 10.0,
            "temperature_min_c": 2.0,
            "precipitation_mm": 0.0,
        }
    )


def test_run_backtest_logs_every_split_to_mlflow(tmp_path):
    training_root = tmp_path / "training"
    training_root.mkdir()
    for month in ["202502", "202503", "202504"]:
        partition = month_partition(month)
        if month == "202504":
            # One extra departure the pattern cannot know about: without it
            # the naive forecast is exact, its MAE is 0, and the ratio
            # against itself would be 0/0.
            partition.loc[0, "quantity"] += 1
        partition.to_parquet(training_root / f"{month}.parquet", index=False)

    store = MlflowStore(tmp_path / "mlflow")
    table = run_backtest(
        ["202502", "202503", "202504"],
        ["seasonal_naive"],
        n_splits=1,
        training_root=training_root,
        store=store,
        weather_df=flat_weather("2025-02-01", "2025-04-30"),
        log=lambda message: None,
    )

    row = table.set_index("model").loc["seasonal_naive"]
    assert row["mae_over_naive"] == pytest.approx(1.0)
    assert row["actual_total"] > 0
    # The naive's hour-of-week mean over 8 exact weeks is the pattern itself,
    # so the forecast for April is nearly perfect.
    assert row["mae"] == pytest.approx(0.0, abs=0.05)

    import mlflow

    store.activate()
    runs = mlflow.search_runs(experiment_names=[EXPERIMENT_NAME])
    names = set(runs["tags.mlflow.runName"])
    assert names == {"seasonal_naive-202504", "comparison"}
    model_run = runs[runs["tags.mlflow.runName"] == "seasonal_naive-202504"].iloc[0]
    assert model_run["params.test_month"] == "202504"
    assert model_run["metrics.mae"] == pytest.approx(row["mae"])
