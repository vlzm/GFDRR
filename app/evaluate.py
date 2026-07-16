"""Two-level evaluation of the demand models through the simulator."""

from __future__ import annotations

import argparse
import pathlib
from collections.abc import Callable
from typing import Literal

import pandas as pd
from eval_comparison import EvalNames, ModelForecast, build_comparison
from runner import RunRequest, build_graph_data, run_and_save

from gbp import artifacts
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
    """Read the held-out month's actual demand from its training partition."""
    counts = load_actual_month(month)
    return counts[counts["quantity"] > 0].reset_index(drop=True)


def ensure_forecast(
    model_name: str,
    month: str,
    horizon_periods: int,
    log: Callable[[str], None] = print,
) -> str:
    """Return the name of the model's forecast for ``month``, building it if missing."""
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
    """Run one evaluation run and save its artifact; skip when it already exists."""
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
    """Build every evaluation run for one held-out month and compare them."""
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
        description="Evaluate the demand models through the simulator."
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
