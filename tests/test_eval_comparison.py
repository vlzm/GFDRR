"""Tests for the comparison bookkeeping of the two-level evaluation.

The heavy path (resolve the scenario, run 744 periods) stays in the evaluation
itself; these tests pin the logic ``app/eval_comparison.py`` owns -- the run
and file names, and the comparison rows -- by passing a fake run and fake
artifact readers, so they run without a simulator run.
"""

import pathlib
import sys
import types

import pandas as pd
import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "app"))

from eval_comparison import (  # noqa: E402  (needs the app folder on sys.path)
    EvalNames,
    ModelForecast,
    build_comparison,
)

CLASSIC = "classic_bike"


def test_full_month_names_drop_the_window_suffix():
    names = EvalNames("202601", horizon_periods=744, month_hours=744)
    assert names.is_full_month
    assert names.prefix == "eval_202601"
    assert names.reference_run == "eval_202601_reference"
    assert names.forecast_run("lightgbm") == "eval_202601_lightgbm_forecast"
    assert names.comparison_csv == "comparison.csv"


def test_shortened_window_names_carry_the_period_count():
    names = EvalNames("202601", horizon_periods=168, month_hours=744)
    assert not names.is_full_month
    assert names.prefix == "eval_202601_168p"
    assert names.reference_run == "eval_202601_168p_reference"
    assert names.forecast_run("sarimax") == "eval_202601_168p_sarimax_forecast"
    assert names.comparison_csv == "comparison_168p.csv"


def _demand(rows: list[tuple[int, str, int]]) -> pd.DataFrame:
    """Build a demand table from (period_id, facility_id, quantity) rows."""
    return pd.DataFrame(
        [
            {"period_id": p, "facility_id": f, "commodity_category": CLASSIC, "quantity": q}
            for p, f, q in rows
        ]
    )


def _panel(rows: list[tuple[str, int, int, int]]) -> pd.DataFrame:
    """Build a run panel from (facility_id, period_id, departed, lost_demand) rows."""
    return pd.DataFrame(
        [
            {"facility_id": f, "period_id": p, "departed": d, "lost_demand": lost}
            for f, p, d, lost in rows
        ]
    )


def _meta(**totals: float) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        violations=[],
        totals=totals,
        initial_inventory_bikes=100,
        station_capacity_docks=200,
    )


def test_build_comparison_assembles_the_reference_and_forecast_rows():
    names = EvalNames("202601", horizon_periods=1, month_hours=744)
    # s1 makes 8 of the 10 actual departures, so it is the one busy station.
    actual_df = _demand([(0, "s1", 8), (0, "s2", 2)])
    model = ModelForecast("m1", "m1_fc", _demand([(0, "s1", 6), (0, "s2", 3)]), dropped_share=0.1)

    reference_panel = _panel([("s1", 0, 8, 0), ("s2", 0, 2, 0)])
    forecast_panel = _panel([("s1", 0, 6, 2), ("s2", 0, 2, 1)])
    panels = {names.reference_run: reference_panel, names.forecast_run("m1"): forecast_panel}
    metas = {
        names.reference_run: _meta(departed=10.0),
        names.forecast_run("m1"): _meta(departed=8.0),
    }

    calls: list[dict] = []

    def fake_run(**kwargs):
        calls.append(kwargs)

    table = build_comparison(
        names,
        actual_df,
        [model],
        run=fake_run,
        load_meta=lambda run_name: metas[run_name],
        load_table=lambda run_name, table_name: panels[run_name],
        log=lambda _msg: None,
    )

    # The reference run faces the actual demand and is sized on itself (no
    # sizing override); the forecast run faces the forecast and is sized on
    # the actual demand -- the replay-state rule.
    assert len(calls) == 2
    assert calls[0]["run_name"] == names.reference_run
    assert calls[0]["demand_source"] == "history"
    assert calls[0]["demand_df"] is actual_df
    assert "sizing_demand_df" not in calls[0]

    assert calls[1]["run_name"] == names.forecast_run("m1")
    assert calls[1]["demand_source"] == "forecast"
    assert calls[1]["forecast_name"] == "m1_fc"
    assert calls[1]["forecast_dropped_share"] == 0.1
    assert calls[1]["demand_df"] is model.forecast_df
    assert calls[1]["sizing_demand_df"] is actual_df

    assert list(table["model"]) == ["actual", "m1"]
    assert list(table["run_kind"]) == ["reference", "forecast"]
    reference_row, forecast_row = table.iloc[0], table.iloc[1]
    assert reference_row["run_name"] == names.reference_run
    assert reference_row["departed"] == 10.0
    assert reference_row["initial_inventory_bikes"] == 100

    # panel_departed_mae: |6-8| at s1, 0 at s2, mean 1.0.
    assert forecast_row["panel_departed_mae"] == pytest.approx(1.0)
    # lost_demand: 2 at the busy s1, 1 at s2, total 3 -> busy share 2/3.
    assert forecast_row["lost_demand_busy_share"] == pytest.approx(2 / 3)
    assert forecast_row["level1_actual_total"] == pytest.approx(10.0)
    assert forecast_row["level1_predicted_total"] == pytest.approx(9.0)


def test_build_comparison_reports_zero_busy_share_when_nothing_is_lost():
    names = EvalNames("202601", horizon_periods=1, month_hours=744)
    actual_df = _demand([(0, "s1", 5)])
    model = ModelForecast("m1", "m1_fc", _demand([(0, "s1", 5)]), dropped_share=0.0)

    panels = {
        names.reference_run: _panel([("s1", 0, 5, 0)]),
        names.forecast_run("m1"): _panel([("s1", 0, 5, 0)]),
    }
    metas = {
        names.reference_run: _meta(departed=5.0),
        names.forecast_run("m1"): _meta(departed=5.0),
    }

    table = build_comparison(
        names,
        actual_df,
        [model],
        run=lambda **_kwargs: None,
        load_meta=lambda run_name: metas[run_name],
        load_table=lambda run_name, table_name: panels[run_name],
        log=lambda _msg: None,
    )
    assert table.iloc[1]["lost_demand_busy_share"] == 0.0
