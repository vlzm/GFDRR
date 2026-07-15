"""Two-level evaluation of the demand models through the simulator (plan, phase 5).

Level 1 scores a forecast against the held-out month's actual counts — the
backtest metrics (``gbp/ml/metrics.py``). Level 2 holds the physical state
fixed and swaps only the demand: the state (initial inventory and dock
capacities) is always the one sized on the month's actual demand, and each
model's forecast runs against it. Any difference in run totals then comes
from the forecast alone, not from a different physical state. For one
held-out month this module saves, over the same period grid, the same OD
matrix, and the same demand universe (Notations.md §11):

- one reference run — the actual demand against the state sized on itself
  (this state is the replay state every other run reuses);
- per model, one replay-state forecast run — the forecast demand against
  that same state (``run_sized_scenario`` with ``sizing_data`` = the actual
  scenario data). The state has no slack beyond what the actual demand
  needed, so overprediction shows up as ``lost_demand`` and ``redirected``,
  and underprediction as departures below the reference.

The scenario (stations, OD matrix, capacities) comes from the canonical trip
CSV, so a demand table can name stations or station-hours the scenario has
never seen. Every demand table is first cut to what the scenario can run
(:func:`restrict_demand_to_scenario`) — the same cut for the actual and every
forecast, so all runs face the same universe and their totals compare.

Every run is a normal run artifact under ``data/runs/`` (the web interface
can show it), named ``eval_<month>_...``. A run that already exists is not
rerun — delete its folder to redo it. The comparison lands in
``data/ml/evaluation/<month>/comparison.csv``, one row per run.

Terminal use::

    python app/evaluate.py --month 202601
    python app/evaluate.py --month 202601 --models seasonal_naive lightgbm
"""

from __future__ import annotations

import argparse
import pathlib
from collections.abc import Callable
from typing import Literal

import artifacts
import pandas as pd
from eval_comparison import EvalNames, ModelForecast, build_comparison
from runner import RunRequest, build_graph_data, run_and_save

from gbp.loaders.dataloader_graph import (
    ResolvedModelData,
    apply_forecast_demand,
    restrict_demand_to_scenario,
)
from gbp.loaders.download import month_bounds, normalize_month
from gbp.ml import forecast
from gbp.ml.data import ml_dir, month_period_grid
from gbp.ml.training import load_actual_month, training_dir


def evaluation_dir(month: str) -> pathlib.Path:
    """Folder of one month's evaluation outputs: ``<ml dir>/evaluation/<month>``."""
    return ml_dir() / "evaluation" / normalize_month(month)


def actual_demand_table(month: str) -> pd.DataFrame:
    """Read the held-out month's actual demand from its training partition.

    The partition already counts departures per ``(period, facility,
    commodity)`` on the month period grid (period ids 0, 1, 2, … from the
    month's first hour — ``month_period_grid``). Like the historical demand
    marginal, only positive rows are kept.
    """
    counts = load_actual_month(month)
    return counts[counts["quantity"] > 0].reset_index(drop=True)


def ensure_forecast(
    model_name: str,
    month: str,
    horizon_periods: int,
    log: Callable[[str], None] = print,
) -> str:
    """Return the name of the model's forecast for ``month``, building it if missing.

    The forecast is named ``<model>_<month>`` and trains on every training
    partition before the month — the same window as the backtest split that
    tests this month. A missing artifact is built for the whole month, so
    one artifact serves any evaluation window. An existing artifact is
    checked, not rebuilt: its horizon must start at the month's first hour
    and cover ``horizon_periods``.
    """
    month = normalize_month(month)
    name = f"{model_name}_{month}"
    start = month_bounds(month)[0]
    if name not in forecast.list_forecasts():
        train_months = [p.stem for p in sorted(training_dir().glob("*.parquet")) if p.stem < month]
        if not train_months:
            raise FileNotFoundError(f"no training partitions before {month}")
        forecast.build_model_forecast(
            model_name,
            train_months,
            horizon_periods=len(month_period_grid(month)),
            forecast_name=name,
            log=log,
        )
    _, meta = forecast.load_forecast(name)
    if pd.Timestamp(meta.t0) != start or meta.horizon_periods < horizon_periods:
        raise ValueError(
            f"forecast {name} covers {meta.horizon_periods} periods from {meta.t0}; "
            f"the evaluation needs {horizon_periods} periods from {start}"
        )
    return name


def _ensure_run(
    graph_data: ResolvedModelData,
    demand_df: pd.DataFrame,
    periods_df: pd.DataFrame,
    *,
    run_name: str,
    demand_source: Literal["history", "forecast"],
    forecast_name: str | None = None,
    forecast_dropped_share: float | None = None,
    sizing_demand_df: pd.DataFrame | None = None,
    root: pathlib.Path | None = None,
    log: Callable[[str], None] = print,
) -> None:
    """Run one evaluation run and save its artifact; skip when it already exists.

    The demand table is put in place with :func:`apply_forecast_demand` (the
    forecast-run path), and the run goes through the same
    :func:`runner.run_and_save` the terminal runner uses -- so an evaluation
    run is a normal run artifact, built the one way. With ``sizing_demand_df``
    the state is sized on that table instead of the run's own demand; the
    evaluation passes the actual demand there, so every forecast runs against
    the replay state (Notations.md §11).
    """
    if run_name in artifacts.list_runs(root):
        log(f"{run_name}: exists, skipping")
        return
    log(f"{run_name}: running {len(periods_df)} periods ...")
    data = apply_forecast_demand(graph_data, demand_df, periods_df)
    sizing_data = None
    if sizing_demand_df is not None:
        sizing_data = apply_forecast_demand(graph_data, sizing_demand_df, periods_df)
    request = RunRequest(
        run_name=run_name,
        number_of_periods=len(periods_df),
        demand_source=demand_source,
        forecast_name=forecast_name,
    )
    run_and_save(
        data,
        request,
        sizing_data=sizing_data,
        forecast_dropped_share=forecast_dropped_share,
        root=root,
        on_progress=log,
    )
    meta = artifacts.load_run_meta(run_name, root)
    log(f"{run_name}: violations={len(meta.violations)} totals={meta.totals}")


def evaluate_month(
    month: str,
    model_names: list[str],
    trips_path: str | None = None,
    root: pathlib.Path | None = None,
    n_periods: int | None = None,
    log: Callable[[str], None] = print,
) -> pd.DataFrame:
    """Build every evaluation run for one held-out month and compare them.

    The steps, in order: resolve the scenario from the canonical trip CSV,
    cut the actual demand and every model's forecast to the scenario
    (:func:`restrict_demand_to_scenario`), save the reference run, then per
    model one replay-state forecast run — the forecast demand against the
    state sized on the actual demand — and read one comparison row off
    every saved run. Level-1 metrics are computed on the restricted
    whole-bike tables — the exact tables the runs consumed.

    ``n_periods`` shortens the window: only the first ``n_periods`` hours of
    the month are compared (default: the whole month). A shortened window
    runs proportionally faster; its runs and its comparison file carry the
    window in their names (``eval_<month>_<N>p_...``,
    ``comparison_<N>p.csv``), so they never mix with the full-month ones.

    Returns the comparison table and writes it to
    ``data/ml/evaluation/<month>/``.
    """
    month = normalize_month(month)
    month_periods_df = month_period_grid(month)
    month_hours = len(month_periods_df)
    horizon_periods = month_hours if n_periods is None else min(n_periods, month_hours)
    periods_df = month_periods_df.iloc[:horizon_periods]
    names = EvalNames(month, horizon_periods, month_hours)

    forecast_names = {
        name: ensure_forecast(name, month, horizon_periods, log) for name in model_names
    }

    log("Resolving the scenario from the canonical trip CSV ...")
    graph_data = build_graph_data(trips_path) if trips_path else build_graph_data()

    actual_month_df = actual_demand_table(month)
    actual_month_df = actual_month_df[actual_month_df["period_id"] < horizon_periods]
    actual_df, dropped = restrict_demand_to_scenario(actual_month_df, graph_data, periods_df)
    log(f"actual demand: {actual_df['quantity'].sum():,} bikes ({dropped:.2%} dropped by the cut)")

    model_forecasts = []
    for model_name in model_names:
        forecast_demand_df, _ = forecast.load_forecast(forecast_names[model_name])
        forecast_demand_df = forecast_demand_df[forecast_demand_df["period_id"] < horizon_periods]
        forecast_df, forecast_dropped = restrict_demand_to_scenario(
            forecast_demand_df, graph_data, periods_df
        )
        model_forecasts.append(
            ModelForecast(model_name, forecast_names[model_name], forecast_df, forecast_dropped)
        )

    def run(
        *,
        run_name: str,
        demand_df: pd.DataFrame,
        demand_source: Literal["history", "forecast"],
        forecast_name: str | None = None,
        forecast_dropped_share: float | None = None,
        sizing_demand_df: pd.DataFrame | None = None,
    ) -> None:
        _ensure_run(
            graph_data,
            demand_df,
            periods_df,
            run_name=run_name,
            demand_source=demand_source,
            forecast_name=forecast_name,
            forecast_dropped_share=forecast_dropped_share,
            sizing_demand_df=sizing_demand_df,
            root=root,
            log=log,
        )

    def load_table(run_name: str, table_name: str) -> pd.DataFrame:
        return artifacts.load_run_table(run_name, table_name, root)

    table = build_comparison(
        names,
        actual_df,
        model_forecasts,
        run=run,
        load_meta=lambda run_name: artifacts.load_run_meta(run_name, root),
        load_table=load_table,
        log=log,
    )

    out = evaluation_dir(month)
    out.mkdir(parents=True, exist_ok=True)
    table.to_csv(out / names.comparison_csv, index=False)
    log(f"Saved {out / names.comparison_csv}")
    return table


def main() -> None:
    """Terminal entry point: one held-out month in, the comparison table out."""
    parser = argparse.ArgumentParser(
        description="Evaluate the demand models through the simulator (plan, phase 5)."
    )
    parser.add_argument("--month", required=True, help="held-out month, as YYYYMM or YYYY-MM")
    parser.add_argument(
        "--models",
        nargs="+",
        default=["seasonal_naive", "sarimax", "lightgbm", "graphsage"],
        help="model families to evaluate (default: all four)",
    )
    parser.add_argument("--trips-path", default=None, help="scenario trip CSV override")
    parser.add_argument(
        "--periods",
        type=int,
        default=None,
        help="hours to evaluate from the month start, e.g. 168 for one week "
        "(default: the whole month)",
    )
    args = parser.parse_args()

    table = evaluate_month(args.month, args.models, args.trips_path, n_periods=args.periods)
    with pd.option_context("display.width", 200):
        print(table.to_string(index=False))


if __name__ == "__main__":
    main()
