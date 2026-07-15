"""Comparison bookkeeping for the two-level evaluation (``app/evaluate.py``).

Three things live here, apart from the heavy scenario resolution and the
744-period simulator run:

- the run and file names of one month's evaluation
  (:class:`EvalNames`): ``eval_<month>_reference``,
  ``eval_<month>_<model>_forecast``, ``comparison.csv`` -- with the ``_<N>p``
  window suffix when only the first ``N`` hours are compared;
- the rule that says which demand each run faces: the reference run runs the
  actual demand sized on itself; each model's replay-state forecast run runs
  the forecast demand against the state sized on that same actual demand
  (Notations.md: reference run, replay-state forecast run);
- the layout of the comparison table -- one row per run, read off the saved
  ``meta.json`` and ``panel`` (:func:`build_comparison`).

The simulator run and the two artifact readers are passed in as functions, so
this logic runs its unit tests in milliseconds without a simulator run: a test
gives a fake ``run`` that writes canned tables and fake ``load_meta`` /
``load_table`` that return them, then checks the names and the rows.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, Protocol

import pandas as pd

from gbp.ml.metrics import busy_facility_ids, forecast_metrics


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
    """The run and file names of one month's evaluation over one window.

    ``horizon_periods`` is the compared window; ``month_hours`` is the whole
    month. A full-month window drops the ``_<N>p`` suffix, so a full-month run
    and a shortened-window run never share a name or a comparison file.
    """

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
    """One model's forecast, already cut to the scenario, ready to run.

    ``forecast_df`` is the restricted demand table the run faces;
    ``dropped_share`` is the share of the raw forecast cut by that restriction,
    recorded in the run's ``meta.json``.
    """

    model_name: str
    forecast_name: str
    forecast_df: pd.DataFrame
    dropped_share: float


def run_row(run_name: str, load_meta: LoadMetaFn) -> dict[str, object]:
    """One comparison row read off a saved run's ``meta.json``: totals plus the sized state.

    The sized state (``initial_inventory_bikes``, ``station_capacity_docks``)
    is precomputed into ``meta.json`` at save time. A run saved before those
    fields existed carries None there -- delete its folder to rebuild it.
    """
    meta = load_meta(run_name)
    return {
        "run_name": run_name,
        "violations": len(meta.violations),
        **meta.totals,
        "initial_inventory_bikes": meta.initial_inventory_bikes,
        "station_capacity_docks": meta.station_capacity_docks,
    }


def panel_departed_mae(panel_df: pd.DataFrame, reference_panel_df: pd.DataFrame) -> float:
    """Mean |departed difference| per station-hour between two run panels."""
    keys = ["facility_id", "period_id"]
    joined = reference_panel_df[keys + ["departed"]].merge(
        panel_df[keys + ["departed"]], on=keys, how="outer", suffixes=("_reference", "")
    )
    joined = joined.fillna({"departed_reference": 0, "departed": 0})
    return float((joined["departed"] - joined["departed_reference"]).abs().mean())


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
    """Run the reference and every forecast run, then read one row off each.

    The steps, in order: run the reference run (the actual demand sized on
    itself), read its panel and its row; then per model run the replay-state
    forecast run (the forecast demand against the state sized on the actual
    demand) and read its row -- the level-1 metrics, the sized-state totals,
    the panel departed MAE against the reference, and the share of lost demand
    that fell on the busy stations.

    The ``run`` function and the two readers are injected: :mod:`evaluate`
    passes the simulator run and the artifact readers; a test passes fakes and
    checks the rows without a simulator run.

    Returns the comparison table -- one row per run, the reference first.
    """
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
        lost = panel.groupby("facility_id", observed=True)["lost_demand"].sum()
        total_lost = float(lost.sum())
        rows.append(
            {
                "month": names.month,
                "periods": names.horizon_periods,
                "model": model.model_name,
                "run_kind": "forecast",
                **run_row(run_name, load_meta),
                **{f"level1_{key}": value for key, value in level_1.items()},
                "panel_departed_mae": panel_departed_mae(panel, reference_panel),
                "lost_demand_busy_share": (
                    float(lost[lost.index.isin(busy)].sum() / total_lost) if total_lost else 0.0
                ),
            }
        )

    return pd.DataFrame(rows)
