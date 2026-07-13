"""Smoke tests for phase 8: the training path and the retraining pipeline.

The training smoke test fits the seasonal naive and the LightGBM families on
the tiny two-station fixture, builds a forecast artifact for each, and checks
the saved forecast demand table against ``HISTORICAL_DEMAND_SCHEMA`` — the
integration contract the simulator reads. The pipeline smoke test runs all
five phase-6 steps (download → build-table → train → backtest → promote) on
fixture CSVs in a temporary raw folder; the bucket lookup is stubbed and the
weather comes from a pre-written year file, so nothing touches the network.
"""

import pandas as pd
import pytest

from gbp.loaders.dataloader_graph import HISTORICAL_DEMAND_SCHEMA
from gbp.loaders.download import month_bounds
from gbp.ml import registry
from gbp.ml.forecast import build_model_forecast, load_forecast
from gbp.ml.pipeline import STEPS, run_pipeline
from gbp.model.journal_schema import schema_violations
from tests.test_ml_models import flat_weather
from tests.test_ml_registry import write_tiny_partitions
from tests.test_ml_training import NEW_HEADER, new_row


# ---------------------------------------------------------------------------
# Training smoke: fit, forecast, check the demand schema
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("family", "params"),
    [
        ("seasonal_naive", {}),
        ("lightgbm", {"num_boost_round": 50, "min_data_in_leaf": 5}),
    ],
)
def test_trained_model_forecast_obeys_the_demand_schema(family, params, tmp_path):
    training_root = tmp_path / "training"
    months = write_tiny_partitions(training_root)

    folder = build_model_forecast(
        family,
        months,
        horizon_periods=24,
        forecast_name=f"smoke_{family}",
        root=tmp_path / "forecasts",
        training_root=training_root,
        weather_df=flat_weather("2025-05-01", "2025-05-01"),
        log=lambda message: None,
        **params,
    )
    assert folder == tmp_path / "forecasts" / f"smoke_{family}"

    demand_df, meta = load_forecast(f"smoke_{family}", tmp_path / "forecasts")
    assert meta.model_name == family
    assert len(demand_df) > 0
    assert schema_violations(HISTORICAL_DEMAND_SCHEMA, demand_df) == []
    # Whole bikes, positive rows only, inside the horizon.
    assert demand_df["quantity"].dtype == "int64"
    assert (demand_df["quantity"] > 0).all()
    assert demand_df["period_id"].between(0, 23).all()


# ---------------------------------------------------------------------------
# Pipeline smoke: the five phase-6 steps end to end on fixture CSVs
# ---------------------------------------------------------------------------
def write_month_csv(raw, month: str) -> None:
    """Write one month of fixture trips: a daily 08:00 pattern that varies by day parity."""
    start, end = month_bounds(month)
    rows = []
    for day in pd.date_range(start, end - pd.Timedelta(days=1), freq="D"):
        rows.append(new_row(f"{day.date()} 08:05:00", "s1", "s2"))
        if day.day % 2 == 0:
            rows.append(new_row(f"{day.date()} 08:40:00", "s1", "s2"))
        else:
            rows.append(new_row(f"{day.date()} 09:10:00", "s2", "s1"))
    (raw / f"{month}-citibike-tripdata_1.csv").write_text("\n".join([NEW_HEADER, *rows]) + "\n")


def write_weather_year(raw) -> None:
    """Cache a full 2025 weather year file so ``load_weather_daily`` never downloads."""
    dates = pd.date_range("2025-01-01", "2025-12-31", freq="D")
    frame = pd.DataFrame(
        {"DATE": dates.strftime("%Y-%m-%d"), "TMAX": 10.0, "TMIN": 2.0, "PRCP": 0.0}
    )
    frame.to_csv(raw / "weather-central-park-2025.csv", index=False)


def test_pipeline_runs_all_five_steps_on_the_fixture(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    raw.mkdir()
    months = ["202502", "202503", "202504"]
    for month in months:
        write_month_csv(raw, month)
    write_weather_year(raw)

    def bucket_has_nothing(month: str) -> list[str]:
        raise ValueError(f"the bucket has no monthly zip for {month}")

    monkeypatch.setattr("gbp.ml.pipeline.month_zip_keys", bucket_has_nothing)

    training_root = tmp_path / "training"
    store = registry.MlflowStore(tmp_path / "mlflow")
    log_path = tmp_path / "pipeline_log.csv"
    notes = []
    settings = {
        "family": "seasonal_naive",
        "n_splits": 1,
        "raw": raw,
        "training_root": training_root,
        "tracking_dir": store.root,
        "log_path": log_path,
        "log": notes.append,
    }

    row = run_pipeline(STEPS, **settings)

    # The trigger asked the bucket and found nothing new to download.
    assert any("no new months published" in note for note in notes)
    # build-table made one partition per fixture month.
    assert sorted(p.stem for p in training_root.glob("*.parquet")) == months
    # train + backtest + promote: the first version became champion with a score.
    assert row is not None and row["promoted"] is True
    champion = store.champion_version()
    assert champion is not None and str(champion.version) == row["candidate_version"]
    assert row["candidate_mae"] is not None and row["candidate_mae"] > 0

    # Rerun with nothing new: every step is idempotent, one more log row says so.
    partition_bytes = (training_root / "202504.parquet").read_bytes()
    rerun = run_pipeline(STEPS, **settings)
    assert rerun is not None and rerun["promoted"] is False
    assert "already the champion" in str(rerun["reason"])
    assert (training_root / "202504.parquet").read_bytes() == partition_bytes
    table = pd.read_csv(log_path)
    assert table["promoted"].tolist() == [True, False]
