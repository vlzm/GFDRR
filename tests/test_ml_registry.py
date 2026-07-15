"""Tests for phase 6: the model registry, the champion alias, the retraining pipeline.

Everything runs on the tiny two-station fixture of ``test_ml_models`` and a
temporary MLflow store, so the registry round trip, the promote rule, and
the pipeline log are checked end to end without real data or network.
"""

import dataclasses

import pandas as pd
import pytest

from gbp.ml import registry
from gbp.ml.data import MlPaths
from gbp.ml.forecast import build_champion_forecast, load_forecast
from gbp.ml.models import create_model
from gbp.ml.pipeline import (
    LOG_COLUMNS,
    append_log_row,
    family_score,
    promote_decision,
    published_missing_months,
    run_pipeline,
)
from tests.test_ml_models import (
    MONDAY,
    WEEK,
    flat_weather,
    month_partition,
    tiny_forecast_input,
    tiny_training_table,
)


@dataclasses.dataclass
class StubVersion:
    """The two fields the promote rule reads off a registry version."""

    version: str
    tags: dict[str, str]


def stub(version: str, family: str = "seasonal_naive") -> StubVersion:
    return StubVersion(version=version, tags={"model_family": family})


def fitted_naive():
    """Fit a seasonal naive — the cheapest model to put in the registry."""
    model = create_model("seasonal_naive")
    model.fit(tiny_training_table())
    return model


def comparison_frame(scores: dict[str, float]) -> pd.DataFrame:
    """Build a comparison table with one mae per model family."""
    return pd.DataFrame({"model": list(scores), "mae": list(scores.values())})


def write_tiny_partitions(root) -> list[str]:
    """Three monthly partitions of the fixture pattern, one bump in the last."""
    root.mkdir()
    months = ["202502", "202503", "202504"]
    for month in months:
        partition = month_partition(month)
        if month == "202504":
            partition.loc[0, "quantity"] += 1
        partition.to_parquet(root / f"{month}.parquet", index=False)
    return months


# ---------------------------------------------------------------------------
# The registry round trip
# ---------------------------------------------------------------------------
def test_registry_round_trip_register_find_promote_load(tmp_path):
    store = registry.MlflowStore(tmp_path / "mlflow")
    months = ["202502", "202503"]

    version = store.register_version(fitted_naive(), train_months=months, data_version="abc123")
    assert str(version.version) == "1"

    # The twin lookup finds the same identity and nothing else.
    found = store.find_version("seasonal_naive", months, "abc123")
    assert found is not None and str(found.version) == "1"
    assert store.find_version("seasonal_naive", months, "def456") is None
    assert store.find_version("lightgbm", months, "abc123") is None

    # No champion until the first promotion.
    assert store.champion_version() is None
    store.promote_to_champion(version)
    champion = store.champion_version()
    assert champion is not None and str(champion.version) == "1"

    # The alias resolves to a model that predicts.
    model, resolved = store.resolve_champion()
    assert str(resolved.version) == "1"
    predicted = model.predict(tiny_forecast_input(MONDAY + 6 * WEEK))
    assert (predicted["quantity"] >= 0).all()


def test_resolve_champion_without_a_champion_raises(tmp_path):
    with pytest.raises(LookupError, match="no champion"):
        registry.MlflowStore(tmp_path / "mlflow").resolve_champion()


def test_promotion_moves_the_alias_and_tags_the_old_champion(tmp_path):
    store = registry.MlflowStore(tmp_path / "mlflow")
    first = store.register_version(fitted_naive(), train_months=["202502"], data_version="v1")
    second = store.register_version(
        fitted_naive(), train_months=["202502", "202503"], data_version="v2"
    )
    store.promote_to_champion(first)
    store.promote_to_champion(second)

    champion = store.champion_version()
    assert champion is not None and str(champion.version) == str(second.version)
    demoted = store.find_version("seasonal_naive", ["202502"], "v1")
    assert demoted is not None and demoted.tags["role"] == "challenger"


# ---------------------------------------------------------------------------
# The comparison storage seam
# ---------------------------------------------------------------------------
def test_latest_comparison_is_none_on_an_empty_store(tmp_path):
    store = registry.MlflowStore(tmp_path / "mlflow")
    assert store.latest_comparison() is None


def test_log_comparison_round_trips_the_table_and_data_version(tmp_path):
    store = registry.MlflowStore(tmp_path / "mlflow")
    first = comparison_frame({"seasonal_naive": 1.0, "lightgbm": 0.8})
    store.log_comparison(first, params={"data_version": "v1", "n_splits": 3})

    saved = store.latest_comparison()
    assert saved is not None
    assert saved.data_version == "v1"
    pd.testing.assert_frame_equal(saved.table, first)

    # A second write is the one that latest_comparison returns.
    second = comparison_frame({"seasonal_naive": 0.5})
    store.log_comparison(second, params={"data_version": "v2"})
    newest = store.latest_comparison()
    assert newest is not None
    assert newest.data_version == "v2"
    pd.testing.assert_frame_equal(newest.table, second)


# ---------------------------------------------------------------------------
# The promote rule
# ---------------------------------------------------------------------------
def test_promote_decision_first_version_needs_no_champion():
    promoted, reason, _, _ = promote_decision(
        stub("1"), None, comparison_frame({"seasonal_naive": 1.0})
    )
    assert promoted
    assert "no champion" in reason


def test_promote_decision_keeps_a_candidate_that_already_is_the_champion():
    promoted, reason, _, _ = promote_decision(stub("3"), stub("3"), None)
    assert not promoted
    assert "already the champion" in reason


def test_promote_decision_better_candidate_wins():
    comparison = comparison_frame({"lightgbm": 0.8, "seasonal_naive": 1.0})
    promoted, reason, candidate_mae, champion_mae = promote_decision(
        stub("2", "lightgbm"), stub("1", "seasonal_naive"), comparison
    )
    assert promoted
    assert (candidate_mae, champion_mae) == (0.8, 1.0)


def test_promote_decision_tie_promotes_the_newer_data_version():
    # Same family on both sides: refit on the same splits scores the same,
    # and the newer data version wins the tie.
    comparison = comparison_frame({"lightgbm": 0.8, "seasonal_naive": 1.0})
    promoted, _, _, _ = promote_decision(stub("2", "lightgbm"), stub("1", "lightgbm"), comparison)
    assert promoted


def test_promote_decision_worse_candidate_stays_challenger():
    comparison = comparison_frame({"graphsage": 1.4, "lightgbm": 0.8, "seasonal_naive": 1.0})
    promoted, reason, _, _ = promote_decision(
        stub("2", "graphsage"), stub("1", "lightgbm"), comparison
    )
    assert not promoted
    assert "challenger" in reason


def test_promote_decision_demands_a_comparison_when_a_champion_exists():
    with pytest.raises(ValueError, match="backtest"):
        promote_decision(stub("2", "lightgbm"), stub("1", "seasonal_naive"), None)


def test_family_score_names_the_missing_family():
    with pytest.raises(ValueError, match="lightgbm"):
        family_score(comparison_frame({"seasonal_naive": 1.0}), "lightgbm")


# ---------------------------------------------------------------------------
# The pipeline log and the trigger
# ---------------------------------------------------------------------------
def test_append_log_row_creates_then_appends(tmp_path):
    path = tmp_path / "pipeline_log.csv"
    append_log_row({"run_at": "2026-07-11T10:00:00", "promoted": True, "reason": "first"}, path)
    append_log_row({"run_at": "2026-07-11T11:00:00", "promoted": False, "reason": "worse"}, path)
    table = pd.read_csv(path)
    assert list(table.columns) == LOG_COLUMNS
    assert table["promoted"].tolist() == [True, False]


def test_published_missing_months_stops_at_the_first_unpublished(monkeypatch):
    published = {"202602", "202603"}

    def fake_keys(month: str) -> list[str]:
        if month not in published:
            raise ValueError(f"the bucket has no monthly zip for {month}")
        return [f"{month}-citibike-tripdata.zip"]

    monkeypatch.setattr("gbp.ml.pipeline.month_zip_keys", fake_keys)
    got = published_missing_months(["202512", "202601"], today=pd.Timestamp("2026-07-11"))
    assert got == ["202602", "202603"]
    assert published_missing_months([], today=pd.Timestamp("2026-07-11")) == []


# ---------------------------------------------------------------------------
# The pipeline end to end on the tiny fixture
# ---------------------------------------------------------------------------
def test_pipeline_promotes_the_first_version_and_keeps_it_on_rerun(tmp_path):
    store = registry.MlflowStore(tmp_path / "mlflow")
    paths = MlPaths.under(tmp_path, tracking=store.root)
    write_tiny_partitions(paths.training)
    log_path = tmp_path / "pipeline_log.csv"
    settings = {
        "family": "seasonal_naive",
        "n_splits": 1,
        "paths": paths,
        "log_path": log_path,
        "weather_df": flat_weather("2025-02-01", "2025-04-30"),
        "log": lambda message: None,
    }

    row = run_pipeline(["train", "backtest", "promote"], **settings)
    assert row is not None and row["promoted"] is True
    champion = store.champion_version()
    assert champion is not None and str(champion.version) == row["candidate_version"]
    assert row["candidate_mae"] is not None  # the backtest score made it into the log

    # Rerun without new data: the version is reused, the backtest is
    # skipped, the champion stays, and one more row records exactly that.
    rerun = run_pipeline(["train", "backtest", "promote"], **settings)
    assert rerun is not None and rerun["promoted"] is False
    assert "already the champion" in str(rerun["reason"])
    assert str(store.latest_registered_version().version) == str(champion.version)
    table = pd.read_csv(log_path)
    assert len(table) == 2
    assert table["promoted"].tolist() == [True, False]


def test_champion_forecast_resolves_the_model_by_alias(tmp_path):
    store = registry.MlflowStore(tmp_path / "mlflow")
    paths = MlPaths.under(tmp_path, tracking=store.root)
    months = write_tiny_partitions(paths.training)
    version = store.register_version(fitted_naive(), train_months=months, data_version="abc123")
    store.promote_to_champion(version)

    build_champion_forecast(
        horizon_periods=24,
        forecast_name="champion_test",
        paths=paths,
        weather_df=flat_weather("2025-05-01", "2025-05-02"),
        log=lambda message: None,
    )

    demand_df, meta = load_forecast("champion_test", paths.forecasts)
    assert meta.model_name == "seasonal_naive"
    assert meta.model_version == str(version.version)
    # The horizon starts right after the newest partition (April 2025).
    assert meta.t0 == pd.Timestamp("2025-05-01").isoformat()
    assert (demand_df["quantity"] > 0).any()
