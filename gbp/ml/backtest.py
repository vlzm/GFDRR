"""Rolling-origin backtest over the training partitions (plan, phase 4).

The backtest (Notations.md §17) is the level-1 evaluation: train on months
``1..k``, forecast month ``k+1``, move the split forward, at least 3 splits.
Rows from the future never appear in training — the splits move only
forward, there are no random splits.

For every split and every model family the run is the same four steps:

1. stack the training partitions of the split's training months;
2. build the forecast input for the held-out month — the history window
   comes from the partitions right before it, the weather is the held-out
   month's actual weather (a perfect weather forecast; real forecasts for a
   true future month will be worse);
3. ``fit`` on the training table, ``predict`` on the forecast input;
4. score the fractional demand against the month's actual counts
   (``gbp/ml/metrics.py``) and log everything to MLflow.

MLflow tracking goes to the local store ``data/ml/mlflow/`` — the runs in
``mlflow.db`` (SQLite), the model files under ``artifacts/``. Browse it with
``mlflow ui --backend-store-uri sqlite:///data/ml/mlflow/mlflow.db``.
Every ``(model, split)``
pair is one MLflow run: parameters (model settings, training months, the
data version — the git commit of the ``.dvc`` files, Notations.md §17),
the metrics of the split, and the fitted model files as artifacts. One
extra run named ``comparison`` holds the cross-model table: mean metrics
per model family next to their ratio against the seasonal naive baseline —
the shared naive month forecast of each held-out month
(``naive_month_prediction`` in ``gbp/ml/forecast.py``).

Terminal use (all four families, three splits, all partitions on disk)::

    python -m gbp.ml.backtest

or explicitly::

    python -m gbp.ml.backtest --months 202502 202503 ... --splits 3
        --models seasonal_naive lightgbm
"""

from __future__ import annotations

import argparse
import dataclasses
import pathlib
import subprocess
import tempfile
import time
from collections.abc import Callable, Sequence

import mlflow
import pandas as pd

from gbp.ml.data import load_weather_daily, month_bounds, month_period_grid, normalize_month
from gbp.ml.forecast import forecast_input, naive_month_prediction
from gbp.ml.metrics import forecast_metrics
from gbp.ml.models import MODEL_FAMILIES, create_model
from gbp.ml.registry import configure_mlflow, ensure_experiment
from gbp.ml.training import (
    load_actual_month,
    load_history_counts,
    load_training_table,
    training_dir,
)

#: The one MLflow experiment all model families log into.
EXPERIMENT_NAME = "demand-backtest"


@dataclasses.dataclass(frozen=True)
class BacktestSplit:
    """One rolling-origin split: train on the months, forecast the test month."""

    train_months: tuple[str, ...]
    test_month: str


def backtest_splits(months: Sequence[str], n_splits: int = 3) -> list[BacktestSplit]:
    """Build the rolling-origin splits: the last ``n_splits`` months are each held out.

    The months must be consecutive calendar months — the history window and
    the SARIMAX daily series both assume no gaps.
    """
    ordered = sorted(normalize_month(m) for m in months)
    periods = pd.PeriodIndex(pd.to_datetime([m + "01" for m in ordered]), freq="M")
    gaps = [str(p) for previous, p in zip(periods, periods[1:], strict=False) if p != previous + 1]
    if gaps:
        raise ValueError(f"months are not consecutive; gaps before: {', '.join(gaps)}")
    if len(ordered) < n_splits + 1:
        raise ValueError(
            f"{len(ordered)} months cannot give {n_splits} splits; "
            "every split needs at least one training month"
        )
    return [
        BacktestSplit(tuple(ordered[:i]), ordered[i])
        for i in range(len(ordered) - n_splits, len(ordered))
    ]


def data_version() -> str:
    """Return the data version: the git commit that last touched the ``.dvc`` files.

    ``-dirty`` is appended when a tracked ``.dvc`` file has uncommitted
    changes; ``unknown`` means git could not answer (no repository, no
    commits touching the files).
    """
    repo = pathlib.Path(__file__).resolve().parents[2]
    tracked = ["data/raw.dvc", "data/ml/training.dvc"]
    try:
        commit = subprocess.run(
            ["git", "log", "-1", "--format=%H", "--", *tracked],
            cwd=repo,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        if not commit:
            return "unknown"
        status = subprocess.run(
            ["git", "status", "--porcelain", "--", *tracked],
            cwd=repo,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        return commit[:12] + ("-dirty" if status else "")
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def month_forecast_input(
    test_month: str,
    training_root: pathlib.Path | None = None,
    raw: pathlib.Path | None = None,
    weather_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build the forecast input for one held-out month.

    The horizon is the month's full hourly grid (``month_period_grid``), the
    history window is read from the training partitions before the month,
    and the weather defaults to the month's actual weather — in a backtest
    that plays the role of a perfect weather forecast.
    """
    horizon = month_period_grid(test_month)
    history = load_history_counts(test_month, training_root)
    if weather_df is None:
        start, end = month_bounds(test_month)
        weather_df = load_weather_daily(start, end - pd.Timedelta(days=1), raw)
    return forecast_input(history, horizon, weather_df)


def comparison_table(
    records: list[dict[str, object]],
    baseline_records: list[dict[str, object]] | None = None,
) -> pd.DataFrame:
    """Average the split metrics per model and compare against the baseline.

    One row per model family: the mean of every metric over the splits.
    ``baseline_records`` holds the per-split metrics of the shared naive
    month forecast (:func:`gbp.ml.forecast.naive_month_prediction`) — the
    same forecast the monitoring alert compares against. With it every row
    gets ``mae_over_naive`` and ``poisson_deviance_over_naive`` — the
    model's mean error divided by the baseline's (below 1.0 beats the
    baseline).
    """
    frame = pd.DataFrame(records)
    table = frame.drop(columns=["test_month"]).groupby("model").mean(numeric_only=True)
    order = [m for m in MODEL_FAMILIES if m in table.index]
    table = table.loc[order + sorted(set(table.index) - set(order))]
    if baseline_records:
        baseline = pd.DataFrame(baseline_records)
        for metric in ["mae", "poisson_deviance"]:
            table[f"{metric}_over_naive"] = table[metric] / baseline[metric].mean()
    return table.reset_index()


def run_backtest(
    months: Sequence[str],
    model_names: Sequence[str] = MODEL_FAMILIES,
    n_splits: int = 3,
    *,
    training_root: pathlib.Path | None = None,
    raw: pathlib.Path | None = None,
    tracking_dir: pathlib.Path | None = None,
    weather_df: pd.DataFrame | None = None,
    log: Callable[[str], None] = print,
) -> pd.DataFrame:
    """Backtest the model families over the same splits, log runs to MLflow.

    Returns the comparison table (:func:`comparison_table`). The order of
    work is split-major: the training table and the forecast input of a
    split are built once and every model reuses them.
    """
    unknown = set(model_names) - set(MODEL_FAMILIES)
    if unknown:
        raise ValueError(f"unknown model families: {', '.join(sorted(unknown))}")
    splits = backtest_splits(months, n_splits)
    version = data_version()

    store = configure_mlflow(tracking_dir)
    ensure_experiment(EXPERIMENT_NAME, store)

    records: list[dict[str, object]] = []
    baseline_records: list[dict[str, object]] = []
    for index, split in enumerate(splits):
        log(
            f"split {index}: train {split.train_months[0]}..{split.train_months[-1]} "
            f"-> test {split.test_month}"
        )
        train_table = load_training_table(list(split.train_months), training_root)
        features = month_forecast_input(split.test_month, training_root, raw, weather_df)
        actual = load_actual_month(split.test_month, training_root)
        naive_df = naive_month_prediction(split.test_month, training_root)
        if naive_df is not None:
            baseline = forecast_metrics(actual, naive_df)
            baseline_records.append({"test_month": split.test_month, **baseline})
            log(f"  naive baseline: mae={baseline['mae']:.4f}")

        for name in model_names:
            started = time.monotonic()
            model = create_model(name)
            model.fit(train_table)
            predicted = model.predict(features)
            metrics = forecast_metrics(actual, predicted)
            elapsed = time.monotonic() - started
            log(f"  {name}: mae={metrics['mae']:.4f} ({elapsed:.0f}s)")

            with mlflow.start_run(run_name=f"{name}-{split.test_month}"):
                mlflow.log_params(
                    {
                        "model": name,
                        "split": index,
                        "train_months": f"{split.train_months[0]}..{split.train_months[-1]}",
                        "test_month": split.test_month,
                        "data_version": version,
                        **model.params(),
                    }
                )
                mlflow.log_metrics({**metrics, "fit_predict_seconds": elapsed})
                with tempfile.TemporaryDirectory() as folder:
                    model.save(pathlib.Path(folder))
                    mlflow.log_artifacts(folder, artifact_path="model")
            records.append({"model": name, "test_month": split.test_month, **metrics})
        del train_table, features

    table = comparison_table(records, baseline_records)
    with mlflow.start_run(run_name="comparison"):
        mlflow.log_params(
            {
                "months": f"{sorted(months)[0]}..{sorted(months)[-1]}",
                "n_splits": n_splits,
                "models": " ".join(model_names),
                "data_version": version,
            }
        )
        with tempfile.TemporaryDirectory() as folder:
            path = pathlib.Path(folder) / "comparison.csv"
            table.to_csv(path, index=False)
            mlflow.log_artifact(str(path))
    return table


def main() -> None:
    """Terminal entry point: months and families in, an MLflow experiment out."""
    parser = argparse.ArgumentParser(
        description="Rolling-origin backtest of the model families, tracked in MLflow."
    )
    parser.add_argument(
        "--months",
        nargs="*",
        default=None,
        help="months to use, as YYYYMM or YYYY-MM (default: every partition on disk)",
    )
    parser.add_argument(
        "--splits", type=int, default=3, help="how many held-out months (default: 3)"
    )
    parser.add_argument(
        "--models",
        nargs="+",
        default=list(MODEL_FAMILIES),
        choices=list(MODEL_FAMILIES),
        help="model families to compare (default: all)",
    )
    args = parser.parse_args()

    months = args.months or [p.stem for p in sorted(training_dir().glob("*.parquet"))]
    if not months:
        raise SystemExit("no training partitions found; build them first")
    table = run_backtest(months, args.models, args.splits)
    print()
    print(table.to_string(index=False, float_format=lambda v: f"{v:.4f}"))


if __name__ == "__main__":
    main()
