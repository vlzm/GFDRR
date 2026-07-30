"""Score saved forecasts against actual months and build the drift report."""

from __future__ import annotations

import argparse
import datetime
import json
import pathlib
from collections.abc import Callable
from typing import Any

import pandas as pd

from domains.citybike.loaders.download import normalize_month
from domains.citybike.ml.data import MlPaths, month_grid
from domains.citybike.ml.features import HISTORY_FEATURES, WEATHER_FEATURES
from domains.citybike.ml.forecast import naive_month_prediction
from domains.citybike.ml.training import load_actual_month, partition_path, training_dir
from gbp.ml.artifact import ForecastMeta, list_forecasts, load_forecast, ml_dir
from gbp.ml.metrics import align_forecast, forecast_metrics

#: How many scored months the rolling MAE of the alert rule averages over.
ROLLING_MONTHS = 3

#: How many rows each side of the drift comparison is sampled down to.
DRIFT_SAMPLE_ROWS = 200_000

#: The columns the drift report checks: weather and demand history. The
#: calendar columns (hour_of_day, day_of_week, month, is_holiday) are left
#: out on purpose — their distributions are set by the calendar, so one
#: monitored month against a mix of training months would flag them every
#: time (the month column of one monitored month is a single value).
DRIFT_COLUMNS = WEATHER_FEATURES + HISTORY_FEATURES

#: When a column counts as drifted: its Wasserstein distance, in units of
#: the reference standard deviation, is at or above this value. Evidently's
#: default is 0.1 — a tenth of a standard deviation, which ordinary
#: month-to-month variation crosses easily.
DRIFT_NUM_THRESHOLD = 0.25

#: The columns of one metrics-table row.
METRICS_COLUMNS = [
    "month",
    "forecast_name",
    "model_name",
    "model_version",
    "n_periods",
    "mae",
    "poisson_deviance",
    "bias",
    "naive_mae",
    "scored_at",
]


def monitoring_dir() -> pathlib.Path:
    """Folder of the monitoring outputs: ``<ml dir>/monitoring``."""
    return ml_dir() / "monitoring"


def metrics_path(root: pathlib.Path | None = None) -> pathlib.Path:
    """Where the metrics table lives: ``<monitoring dir>/metrics.parquet``."""
    return (root or monitoring_dir()) / "metrics.parquet"


def load_metrics(root: pathlib.Path | None = None) -> pd.DataFrame:
    """Read the metrics table; an empty table with the right columns when none exists."""
    path = metrics_path(root)
    if not path.exists():
        return pd.DataFrame(columns=METRICS_COLUMNS)
    return pd.read_parquet(path)


def append_metrics(rows: pd.DataFrame, root: pathlib.Path | None = None) -> pathlib.Path:
    """Append rows to the metrics table, replacing rows with the same keys."""
    path = metrics_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = load_metrics(root)
    rows = rows[METRICS_COLUMNS]
    if table.empty:
        table = rows
    else:
        keys = set(zip(rows["forecast_name"], rows["month"], strict=True))
        stale = [key in keys for key in zip(table["forecast_name"], table["month"], strict=True)]
        table = pd.concat([table[~pd.Series(stale, index=table.index)], rows], ignore_index=True)
    table = table.sort_values(["month", "forecast_name"]).reset_index(drop=True)
    table.to_parquet(path, index=False)
    return path


def month_period_map(meta: ForecastMeta, month: str) -> pd.DataFrame:
    """Map a forecast's period ids onto the month's hourly period ids."""
    mapping = meta.grid.align_to(month_grid(month))
    return mapping.rename(columns={"other_period_id": "month_period_id"})


def score_forecast_against_month(
    forecast_name: str,
    month: str,
    actual_df: pd.DataFrame,
    naive_df: pd.DataFrame | None = None,
    forecasts_root: pathlib.Path | None = None,
) -> dict[str, object] | None:
    """Score one saved forecast against one actual month; None when they do not overlap."""
    demand_df, meta = load_forecast(forecast_name, forecasts_root)
    mapping = month_period_map(meta, month)
    if mapping.empty:
        return None
    predicted = (
        demand_df.merge(mapping, on="period_id")
        .drop(columns="period_id")
        .rename(columns={"month_period_id": "period_id"})
    )
    covered = set(mapping["month_period_id"])
    actual = actual_df[actual_df["period_id"].isin(covered)]
    scores = forecast_metrics(actual, predicted)
    aligned = align_forecast(actual, predicted)
    row: dict[str, object] = {
        "month": normalize_month(month),
        "forecast_name": forecast_name,
        "model_name": meta.model_name,
        "model_version": meta.model_version,
        "n_periods": len(mapping),
        "mae": scores["mae"],
        "poisson_deviance": scores["poisson_deviance"],
        "bias": float((aligned["predicted"] - aligned["actual"]).mean()),
        "naive_mae": float("nan"),
        "scored_at": datetime.datetime.now().isoformat(timespec="seconds"),
    }
    if naive_df is not None:
        naive_part = naive_df[naive_df["period_id"].isin(covered)]
        row["naive_mae"] = forecast_metrics(actual, naive_part)["mae"]
    return row


def score_month(
    month: str,
    *,
    paths: MlPaths | None = None,
    log: Callable[[str], None] = print,
) -> pd.DataFrame:
    """Score every saved forecast that covers the month; append the rows to the metrics table."""
    paths = paths or MlPaths.resolve()
    month = normalize_month(month)
    actual = load_actual_month(month, paths.training)
    naive = naive_month_prediction(month, paths.training)
    if naive is None:
        log(f"{month}: no earlier partitions, the naive baseline stays missing")
    rows = []
    for name in list_forecasts(paths.forecasts):
        row = score_forecast_against_month(name, month, actual, naive, paths.forecasts)
        if row is None:
            log(f"{name}: horizon does not touch {month}, skipping")
            continue
        log(
            f"{name}: mae={row['mae']:.4f} bias={row['bias']:+.4f} "
            f"naive_mae={row['naive_mae']:.4f} over {row['n_periods']} periods"
        )
        rows.append(row)
    if not rows:
        return pd.DataFrame(columns=METRICS_COLUMNS)
    table = pd.DataFrame(rows, columns=METRICS_COLUMNS)
    path = append_metrics(table, paths.monitoring)
    log(f"{month}: {len(table)} rows -> {path}")
    return table


def metric_history(metrics_df: pd.DataFrame, rolling_months: int = ROLLING_MONTHS) -> pd.DataFrame:
    """Aggregate the metrics table per model version and month, with the degraded mark."""
    columns = [
        "model_name",
        "model_version",
        "month",
        "mae",
        "poisson_deviance",
        "bias",
        "naive_mae",
        "rolling_mae",
        "degraded",
    ]
    if metrics_df.empty:
        return pd.DataFrame(columns=columns)
    history = (
        metrics_df.groupby(["model_name", "model_version", "month"], as_index=False)[
            ["mae", "poisson_deviance", "bias", "naive_mae"]
        ]
        .mean()
        .sort_values(["model_name", "model_version", "month"])
    )
    history["rolling_mae"] = history.groupby(["model_name", "model_version"])["mae"].transform(
        lambda mae: mae.rolling(rolling_months, min_periods=1).mean()
    )
    history["degraded"] = history["rolling_mae"] > history["naive_mae"]
    return history[columns].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Drift report
# ---------------------------------------------------------------------------
def drift_report_paths(
    month: str, root: pathlib.Path | None = None
) -> tuple[pathlib.Path, pathlib.Path]:
    """Where one month's drift report lives: the HTML file and the JSON summary."""
    base = root or monitoring_dir()
    month = normalize_month(month)
    return base / f"drift_{month}.html", base / f"drift_{month}.json"


def list_drift_summaries(root: pathlib.Path | None = None) -> list[dict[str, Any]]:
    """Read every saved drift summary, newest month first."""
    base = root or monitoring_dir()
    if not base.exists():
        return []
    summaries = [
        json.loads(path.read_text()) for path in sorted(base.glob("drift_*.json"), reverse=True)
    ]
    return summaries


def _sample_rows(frame: pd.DataFrame, n: int, seed: int) -> pd.DataFrame:
    """Take at most ``n`` rows, deterministically."""
    if len(frame) <= n:
        return frame
    return frame.sample(n=n, random_state=seed)


def _drift_summary(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Pull the headline numbers and the per-column table out of an Evidently snapshot."""
    drifted_count = 0
    drifted_share = float("nan")
    columns: list[dict[str, Any]] = []
    for metric in snapshot["metrics"]:
        config = metric.get("config", {})
        kind = str(config.get("type", ""))
        if kind.endswith("DriftedColumnsCount"):
            drifted_count = int(metric["value"]["count"])
            drifted_share = float(metric["value"]["share"])
        elif kind.endswith("ValueDrift"):
            value = float(metric["value"])
            threshold = float(config.get("threshold", float("nan")))
            columns.append(
                {
                    "column": str(config.get("column")),
                    "method": str(config.get("method", "")),
                    "value": value,
                    "threshold": threshold,
                    "drifted": bool(value >= threshold),
                }
            )
    return {"drifted_count": drifted_count, "drifted_share": drifted_share, "columns": columns}


def drift_report(
    month: str,
    *,
    paths: MlPaths | None = None,
    sample_rows: int = DRIFT_SAMPLE_ROWS,
    seed: int = 0,
    log: Callable[[str], None] = print,
) -> pathlib.Path:
    """Compare the month's feature distributions against the champion's training data."""
    # Imported here, not at the top: the registry drags in MLflow and
    # Evidently is heavy — the monitoring page imports this module without
    # ever building a report.
    from evidently import Report
    from evidently.presets import DataDriftPreset

    from domains.citybike.ml.ops.registry import MlflowStore

    paths = paths or MlPaths.resolve()
    month = normalize_month(month)
    champion = MlflowStore(paths.tracking).champion_version()
    if champion is None:
        raise LookupError(
            "the registry has no champion yet; run the retraining pipeline "
            "first (python -m domains.citybike.ml.ops.pipeline)"
        )
    train_months = str(champion.tags.get("train_months", "")).split(",")
    reference_months = [m for m in train_months if m and m != month]
    if not reference_months:
        raise LookupError(
            f"the champion's training months give no reference besides {month} itself"
        )

    current = pd.read_parquet(partition_path(month, paths.training), columns=DRIFT_COLUMNS)
    current = _sample_rows(current, sample_rows, seed)
    per_month = -(-sample_rows // len(reference_months))
    frames = []
    for reference_month in reference_months:
        path = partition_path(reference_month, paths.training)
        if not path.exists():
            raise FileNotFoundError(
                f"the champion trained on {reference_month} but its partition is gone; "
                "rebuild it first (python -m domains.citybike.ml.training)"
            )
        frames.append(_sample_rows(pd.read_parquet(path, columns=DRIFT_COLUMNS), per_month, seed))
    reference = pd.concat(frames, ignore_index=True)

    log(
        f"drift {month}: {len(current):,} current rows vs {len(reference):,} reference rows "
        f"({reference_months[0]}..{reference_months[-1]}, champion v{champion.version})"
    )
    # A distance method, so one reading holds regardless of sample size: the
    # column drifted when the distance >= DRIFT_NUM_THRESHOLD. Every drift
    # column is numeric, so only the numeric method is set.
    report = Report(
        [
            DataDriftPreset(
                columns=DRIFT_COLUMNS,
                num_method="wasserstein",
                num_threshold=DRIFT_NUM_THRESHOLD,
            )
        ]
    )
    snapshot = report.run(reference_data=reference, current_data=current)

    html_path, summary_path = drift_report_paths(month, paths.monitoring)
    html_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot.save_html(str(html_path))
    summary = {
        "month": month,
        "champion_family": str(champion.tags.get("model_family", "")),
        "champion_version": str(champion.version),
        "reference_months": reference_months,
        "reference_rows": int(len(reference)),
        "current_rows": int(len(current)),
        "created_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "html_report": html_path.name,
        **_drift_summary(snapshot.dict()),
    }
    summary_path.write_text(json.dumps(summary, indent=2))
    log(
        f"drift {month}: {summary['drifted_count']} of {len(DRIFT_COLUMNS)} drift "
        f"columns drifted -> {summary_path}"
    )
    return summary_path


def main() -> None:
    """Terminal entry point: score one actual month and build its drift report."""
    parser = argparse.ArgumentParser(
        description="Score saved forecasts against an actual month and build the drift report."
    )
    parser.add_argument(
        "--month",
        default=None,
        help="the actual month, as YYYYMM or YYYY-MM (default: newest training partition)",
    )
    parser.add_argument("--no-drift", action="store_true", help="skip the drift report")
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=DRIFT_SAMPLE_ROWS,
        help=f"rows per side of the drift comparison (default: {DRIFT_SAMPLE_ROWS})",
    )
    args = parser.parse_args()

    month = args.month
    if month is None:
        partitions = sorted(training_dir().glob("*.parquet"))
        if not partitions:
            raise SystemExit(
                "no training partitions; build them first (python -m domains.citybike.ml.training)"
            )
        month = partitions[-1].stem

    rows = score_month(month)
    if rows.empty:
        print(f"{normalize_month(month)}: no saved forecast covers this month")
    if args.no_drift:
        return
    try:
        drift_report(month, sample_rows=args.sample_rows)
    except LookupError as error:
        print(f"drift report skipped: {error}")


if __name__ == "__main__":
    main()
