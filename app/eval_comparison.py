"""Comparison bookkeeping for the two-level evaluation."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, Protocol

import pandas as pd

from gbp.ml.metrics import (
    busy_facility_ids,
    forecast_metrics,
    lost_demand_busy_share,
    panel_departed_mae,
)


class RunMetaLike(Protocol):
    """The part of a saved run's ``meta.json`` the comparison rows read."""

    violations: list[str]
    totals: dict[str, float]
    initial_inventory_bikes: int | None
    station_capacity_docks: int | None


#: Run one evaluation run and save its artifact; a no-op when it already
#: exists. ``sizing_demand_df`` sizes the state on a different demand table
#: than the run faces (the replay-state forecast run).
RunFn = Callable[..., None]
LoadMetaFn = Callable[[str], RunMetaLike]
LoadTableFn = Callable[[str, str], pd.DataFrame]


@dataclass(frozen=True)
class EvalNames:
    """The run and file names of one month's evaluation over one window."""

    month: str
    horizon_periods: int
    month_hours: int

    @property
    def is_full_month(self) -> bool:
        """True when the window covers the whole month."""
        return self.horizon_periods == self.month_hours

    @property
    def prefix(self) -> str:
        """Shared run-name start: ``eval_<month>`` or ``eval_<month>_<N>p``."""
        if self.is_full_month:
            return f"eval_{self.month}"
        return f"eval_{self.month}_{self.horizon_periods}p"

    @property
    def reference_run(self) -> str:
        """Name of the reference run (the actual demand sized on itself)."""
        return f"{self.prefix}_reference"

    def forecast_run(self, model_name: str) -> str:
        """Name of one model's replay-state forecast run."""
        return f"{self.prefix}_{model_name}_forecast"

    @property
    def comparison_csv(self) -> str:
        """File name of the comparison table for this window."""
        if self.is_full_month:
            return "comparison.csv"
        return f"comparison_{self.horizon_periods}p.csv"


@dataclass(frozen=True)
class ModelForecast:
    """One model's forecast, already cut to the scenario, ready to run."""

    model_name: str
    forecast_name: str
    forecast_df: pd.DataFrame
    dropped_share: float


def run_row(run_name: str, load_meta: LoadMetaFn) -> dict[str, object]:
    """One comparison row read off a saved run's ``meta.json``: totals plus the sized state."""
    meta = load_meta(run_name)
    return {
        "run_name": run_name,
        "violations": len(meta.violations),
        **meta.totals,
        "initial_inventory_bikes": meta.initial_inventory_bikes,
        "station_capacity_docks": meta.station_capacity_docks,
    }


def build_comparison(
    names: EvalNames,
    actual_df: pd.DataFrame,
    model_forecasts: list[ModelForecast],
    *,
    run: RunFn,
    load_meta: LoadMetaFn,
    load_table: LoadTableFn,
    log: Callable[[str], None] = print,
) -> pd.DataFrame:
    """Run the reference and every forecast run, then read one row off each."""
    busy = busy_facility_ids(actual_df)

    reference_source: Literal["history"] = "history"
    run(
        run_name=names.reference_run,
        demand_df=actual_df,
        demand_source=reference_source,
    )
    reference_panel = load_table(names.reference_run, "panel")
    rows: list[dict[str, object]] = [
        {
            "month": names.month,
            "periods": names.horizon_periods,
            "model": "actual",
            "run_kind": "reference",
            **run_row(names.reference_run, load_meta),
        }
    ]

    for model in model_forecasts:
        level_1 = forecast_metrics(actual_df, model.forecast_df)
        log(
            f"{model.model_name}: forecast {model.forecast_df['quantity'].sum():,} bikes "
            f"({model.dropped_share:.2%} dropped by the cut), mae={level_1['mae']:.4f}"
        )

        # The replay-state forecast run: the state is sized on the actual
        # demand (the same state the reference run used), only the demand
        # table is the model's.
        run_name = names.forecast_run(model.model_name)
        run(
            run_name=run_name,
            demand_df=model.forecast_df,
            demand_source="forecast",
            forecast_name=model.forecast_name,
            forecast_dropped_share=model.dropped_share,
            sizing_demand_df=actual_df,
        )
        panel = load_table(run_name, "panel")
        rows.append(
            {
                "month": names.month,
                "periods": names.horizon_periods,
                "model": model.model_name,
                "run_kind": "forecast",
                **run_row(run_name, load_meta),
                **{f"level1_{key}": value for key, value in level_1.items()},
                "panel_departed_mae": panel_departed_mae(panel, reference_panel),
                "lost_demand_busy_share": lost_demand_busy_share(panel, busy),
            }
        )

    return pd.DataFrame(rows)
