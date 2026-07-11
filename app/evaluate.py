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

import artifacts
import pandas as pd
from runner import build_graph_data

from gbp.consumers.simulator import run_sized_scenario
from gbp.loaders.dataloader_graph import (
    ResolvedModelData,
    apply_forecast_demand,
    get_forecast_periods_df,
    hour_of_week,
)
from gbp.ml import forecast
from gbp.ml.data import ml_dir, month_bounds, normalize_month
from gbp.ml.metrics import busy_facility_ids, forecast_metrics
from gbp.ml.training import partition_path, training_dir

#: The columns of a demand table (``HISTORICAL_DEMAND_SCHEMA`` order).
DEMAND_COLUMNS = ["period_id", "facility_id", "commodity_category", "quantity"]


def evaluation_dir(month: str) -> pathlib.Path:
    """Folder of one month's evaluation outputs: ``<ml dir>/evaluation/<month>``."""
    return ml_dir() / "evaluation" / normalize_month(month)


def actual_demand_table(month: str) -> pd.DataFrame:
    """Read the held-out month's actual demand from its training partition.

    The partition already counts departures per ``(period, facility,
    commodity)`` on the month's hourly grid (period ids 0, 1, 2, … from the
    month's first hour — the same grid :func:`get_forecast_periods_df` builds
    for the month). Like the historical demand marginal, only positive rows
    are kept.
    """
    path = partition_path(month)
    if not path.exists():
        raise FileNotFoundError(
            f"no training partition for {month}; build it first (python -m gbp.ml.training)"
        )
    counts = pd.read_parquet(path, columns=DEMAND_COLUMNS)
    return counts[counts["quantity"] > 0].reset_index(drop=True)


def restrict_demand_to_scenario(
    demand_df: pd.DataFrame,
    graph_data: ResolvedModelData,
    periods_df: pd.DataFrame,
) -> tuple[pd.DataFrame, float]:
    """Keep the demand rows the scenario can run; report the dropped share.

    A forecast run maps the scenario's OD matrix onto the forecast periods by
    hour of week, so a demand row can only run when its ``(facility,
    commodity, hour of week)`` has OD rows in the scenario. Rows at stations
    the scenario has never seen have no OD rows either, so one rule covers
    both. ``apply_forecast_demand`` refuses exactly the rows this cut drops.

    Parameters
    ----------
    demand_df : pandas.DataFrame
        A demand table on the ``periods_df`` grid.
    graph_data : ResolvedModelData
        The scenario whose OD matrix the run will use.
    periods_df : pandas.DataFrame
        The period grid of ``demand_df`` (maps ``period_id`` to wall-clock).

    Returns
    -------
    tuple of (pandas.DataFrame, float)
        The kept rows (same columns, sorted) and the dropped share of the
        demand total (0.0 when nothing was dropped).
    """
    od = graph_data.historical_od_matrix_df.merge(
        graph_data.periods_df[["period_id", "start_timestamp"]], on="period_id"
    )
    covered = (
        od.assign(hour_of_week=hour_of_week(od["start_timestamp"]))[
            ["source_id", "commodity_category", "hour_of_week"]
        ]
        .drop_duplicates()
        .rename(columns={"source_id": "facility_id"})
    )
    rows = demand_df.merge(periods_df[["period_id", "start_timestamp"]], on="period_id")
    rows["hour_of_week"] = hour_of_week(rows["start_timestamp"])
    kept = rows.merge(covered, on=["facility_id", "commodity_category", "hour_of_week"])
    total = demand_df["quantity"].sum()
    dropped_share = float(1.0 - kept["quantity"].sum() / total) if total else 0.0
    kept = kept[DEMAND_COLUMNS].sort_values(DEMAND_COLUMNS[:3]).reset_index(drop=True)
    return kept, dropped_share


def ensure_forecast(
    model_name: str,
    month: str,
    horizon_periods: int,
    log: Callable[[str], None] = print,
) -> str:
    """Return the name of the model's forecast for ``month``, building it if missing.

    The forecast is named ``<model>_<month>`` and trains on every training
    partition before the month — the same window as the backtest split that
    tests this month. An existing artifact is checked, not rebuilt: its
    horizon must start at the month's first hour and cover the month.
    """
    month = normalize_month(month)
    name = f"{model_name}_{month}"
    if name not in forecast.list_forecasts():
        train_months = [p.stem for p in sorted(training_dir().glob("*.parquet")) if p.stem < month]
        if not train_months:
            raise FileNotFoundError(f"no training partitions before {month}")
        forecast.build_model_forecast(
            model_name,
            train_months,
            horizon_periods=horizon_periods,
            forecast_name=name,
            log=log,
        )
    _, meta = forecast.load_forecast(name)
    start, _ = month_bounds(month)
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
    demand_source: str,
    forecast_name: str | None = None,
    sizing_demand_df: pd.DataFrame | None = None,
    root: pathlib.Path | None = None,
    log: Callable[[str], None] = print,
) -> None:
    """Run one evaluation run and save its artifact; skip when it already exists.

    The demand goes in through :func:`apply_forecast_demand` — the forecast-run
    path — and the artifact is saved exactly as the runner saves one. With
    ``sizing_demand_df`` the state is sized on that table instead of the run's
    own demand (a forecast-sized run).
    """
    if run_name in artifacts.list_runs(root):
        log(f"{run_name}: exists, skipping")
        return
    log(f"{run_name}: running {len(periods_df)} periods ...")
    data = apply_forecast_demand(graph_data, demand_df, periods_df)
    sizing_data = None
    if sizing_demand_df is not None:
        sizing_data = apply_forecast_demand(graph_data, sizing_demand_df, periods_df)
    result = run_sized_scenario(
        data,
        scenario_id=run_name,
        number_of_periods=len(periods_df),
        validate=False,
        sizing_data=sizing_data,
    )
    tables = artifacts.build_run_tables(
        result.simulated_flows_df,
        initial_inventory=result.initial_inventory_df,
        facilities=data.facilities_df,
        facilities_geo=data.facilities_geo_df,
        facilities_capacities=result.facilities_capacities_df,
        rates=data.commodities_categories_rates_df,
        period_len=data.period_len,
        routes=data.routes,
    )
    meta = artifacts.build_meta(
        tables,
        run_name=run_name,
        demand_scale_factor=1.0,
        sizing_scale_factor=1.0,
        number_of_periods=len(periods_df),
        period_len_hours=data.period_len / pd.Timedelta(hours=1),
        routing_mode=data.routing_mode,
        t0=data.t0,
        inputs=[pathlib.Path(data.trips_path).name],
        violations=result.violations,
        demand_source=demand_source,
        forecast_name=forecast_name,
    )
    artifacts.save_run(run_name, tables, meta, root)
    log(f"{run_name}: violations={len(result.violations)} totals={meta.totals}")


def _panel_departed_mae(panel_df: pd.DataFrame, reference_panel_df: pd.DataFrame) -> float:
    """Mean |departed difference| per station-hour between two run panels."""
    keys = ["facility_id", "period_id"]
    joined = reference_panel_df[keys + ["departed"]].merge(
        panel_df[keys + ["departed"]], on=keys, how="outer", suffixes=("_reference", "")
    )
    joined = joined.fillna({"departed_reference": 0, "departed": 0})
    return float((joined["departed"] - joined["departed_reference"]).abs().mean())


def _run_row(run_name: str, root: pathlib.Path | None = None) -> dict[str, object]:
    """One comparison row read off a saved run: totals plus the sized state.

    The sized state is recovered from the artifact: the initial inventory is
    the panel's period-0 ``quantity_sop`` sum, the dock capacity is the
    ``capacity`` sum over stations in the facilities table.
    """
    meta = artifacts.load_run_meta(run_name, root)
    panel = artifacts.load_run_table(run_name, "panel", root)
    facilities = artifacts.load_run_table(run_name, "facilities", root)
    stations = facilities[facilities["facility_category"] == "station"]
    return {
        "run_name": run_name,
        "violations": len(meta.violations),
        **meta.totals,
        "initial_inventory_bikes": int(panel.loc[panel["period_id"] == 0, "quantity_sop"].sum()),
        "station_capacity_docks": int(stations["capacity"].fillna(0).sum()),
    }


def evaluate_month(
    month: str,
    model_names: list[str],
    trips_path: str | None = None,
    root: pathlib.Path | None = None,
    log: Callable[[str], None] = print,
) -> pd.DataFrame:
    """Build every evaluation run for one held-out month and compare them.

    The steps, in order: resolve the scenario from the canonical trip CSV,
    cut the actual demand and every model's forecast to the scenario
    (:func:`restrict_demand_to_scenario`), save the reference run, then per
    model the forecast run and the forecast-sized run, and read one
    comparison row off every saved run. Level-1 metrics are computed on the
    restricted whole-bike tables — the exact tables the runs consumed.

    Returns the comparison table and writes it to
    ``data/ml/evaluation/<month>/comparison.csv``.
    """
    month = normalize_month(month)
    start, end = month_bounds(month)
    horizon_periods = int((end - start) / pd.Timedelta(hours=1))
    periods_df = get_forecast_periods_df(start, horizon_periods, pd.Timedelta(hours=1))

    forecast_names = {
        name: ensure_forecast(name, month, horizon_periods, log) for name in model_names
    }

    log("Resolving the scenario from the canonical trip CSV ...")
    graph_data = build_graph_data(trips_path) if trips_path else build_graph_data()

    actual_df, dropped = restrict_demand_to_scenario(
        actual_demand_table(month), graph_data, periods_df
    )
    log(f"actual demand: {actual_df['quantity'].sum():,} bikes ({dropped:.2%} dropped by the cut)")
    busy = busy_facility_ids(actual_df)

    reference_name = f"eval_{month}_reference"
    _ensure_run(
        graph_data,
        actual_df,
        periods_df,
        run_name=reference_name,
        demand_source="history",
        root=root,
        log=log,
    )
    reference_panel = artifacts.load_run_table(reference_name, "panel", root)
    rows = [
        {
            "month": month,
            "model": "actual",
            "run_kind": "reference",
            **_run_row(reference_name, root),
        }
    ]

    for model_name in model_names:
        forecast_demand_df, _ = forecast.load_forecast(forecast_names[model_name])
        forecast_df, dropped = restrict_demand_to_scenario(
            forecast_demand_df, graph_data, periods_df
        )
        level_1 = forecast_metrics(actual_df, forecast_df)
        log(
            f"{model_name}: forecast {forecast_df['quantity'].sum():,} bikes "
            f"({dropped:.2%} dropped by the cut), mae={level_1['mae']:.4f}"
        )

        run_name = f"eval_{month}_{model_name}_forecast"
        _ensure_run(
            graph_data,
            forecast_df,
            periods_df,
            run_name=run_name,
            demand_source="forecast",
            forecast_name=forecast_names[model_name],
            root=root,
            log=log,
        )
        panel = artifacts.load_run_table(run_name, "panel", root)
        rows.append(
            {
                "month": month,
                "model": model_name,
                "run_kind": "forecast",
                **_run_row(run_name, root),
                **{f"level1_{key}": value for key, value in level_1.items()},
                "panel_departed_mae": _panel_departed_mae(panel, reference_panel),
            }
        )

        run_name = f"eval_{month}_{model_name}_forecast_sized"
        _ensure_run(
            graph_data,
            actual_df,
            periods_df,
            run_name=run_name,
            demand_source="history",
            forecast_name=forecast_names[model_name],
            sizing_demand_df=forecast_df,
            root=root,
            log=log,
        )
        panel = artifacts.load_run_table(run_name, "panel", root)
        lost = panel.groupby("facility_id", observed=True)["lost_demand"].sum()
        total_lost = float(lost.sum())
        rows.append(
            {
                "month": month,
                "model": model_name,
                "run_kind": "forecast_sized",
                **_run_row(run_name, root),
                "lost_demand_busy_share": (
                    float(lost[lost.index.isin(busy)].sum() / total_lost) if total_lost else 0.0
                ),
            }
        )

    table = pd.DataFrame(rows)
    out = evaluation_dir(month)
    out.mkdir(parents=True, exist_ok=True)
    table.to_csv(out / "comparison.csv", index=False)
    log(f"Saved {out / 'comparison.csv'}")
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
    args = parser.parse_args()

    table = evaluate_month(args.month, args.models, args.trips_path)
    with pd.option_context("display.width", 200):
        print(table.to_string(index=False))


if __name__ == "__main__":
    main()
