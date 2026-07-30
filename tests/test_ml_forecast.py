"""Tests for the forecast builders and the scenario running on what they build.

The seasonal naive through the model interface, the artifact save/load round
trip, and the golden path: a forecast run on a synthetic scenario finishes
with zero invariant violations and departs exactly the forecast demand. Every
fixture here runs a real model, so the tests live with the domain that owns
the model families; the demand-table helpers they call are tested in
``tests/test_forecast.py``.
"""

import pandas as pd
import pytest

from domains.citybike.ml import forecast
from gbp.consumers.simulator import run_sized_scenario
from gbp.ml.artifact import (
    ForecastMeta,
    apply_saved_forecast,
    counts_from_demand,
    forecast_periods_from_meta,
    list_forecasts,
    load_forecast,
    round_forecast_demand,
    save_forecast,
)
from gbp.ml.model import create_model
from gbp.model.dataloader_graph import apply_forecast_demand, get_forecast_periods_df
from tests import scenarios

CLASSIC = "classic_bike"

#: 2026-01-05 is a Monday, so hour_of_week(t0 + h hours) == h for a grid
#: starting here — the arithmetic in the tests below stays readable.
MONDAY = pd.Timestamp("2026-01-05")


def hourly_periods(t0: pd.Timestamp, n: int) -> pd.DataFrame:
    return get_forecast_periods_df(t0, n, pd.Timedelta(hours=1))


def demand_table(rows: list[tuple[int, str, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "period_id": [r[0] for r in rows],
            "facility_id": [r[1] for r in rows],
            "commodity_category": CLASSIC,
            "quantity": [r[2] for r in rows],
        }
    )


# ---------------------------------------------------------------------------
# Seasonal naive (through the model interface)
# ---------------------------------------------------------------------------
def seasonal_naive_fractional(
    history: pd.DataFrame, periods: pd.DataFrame, horizon: pd.DataFrame
) -> pd.DataFrame:
    """Run the interface path: counts grid -> forecast input -> predict."""
    counts = counts_from_demand(history, periods)
    model = create_model("seasonal_naive")
    model.fit(counts)
    return model.predict(forecast.forecast_input(counts, horizon))


def test_seasonal_naive_averages_same_hour_of_week():
    # Two weeks of history. Monday 08:00 sees 2 then 4 departures at s1
    # (mean 3.0); Tuesday 09:00 sees 6 in week one and nothing in week two —
    # the empty week is a real zero observation, so the mean is 3.0, not 6.0.
    periods = hourly_periods(MONDAY, 2 * 168)
    history = demand_table([(8, "s1", 2), (168 + 8, "s1", 4), (33, "s1", 6)])
    horizon = hourly_periods(MONDAY + pd.Timedelta(days=14), 168)

    out = seasonal_naive_fractional(history, periods, horizon)

    positive = out[out["quantity"] > 0].set_index("period_id")["quantity"]
    assert positive[8] == 3.0
    assert positive[33] == 3.0
    # No other hour of week ever saw a departure, so every other row is 0.
    assert sorted(positive.index) == [8, 33]
    assert len(out) == 168  # the full horizon grid: one facility, one commodity


# ---------------------------------------------------------------------------
# The forecast artifact
# ---------------------------------------------------------------------------
def test_forecast_artifact_round_trip(tmp_path):
    periods = hourly_periods(MONDAY, 168)
    history = demand_table([(8, "s1", 2)])

    folder = forecast.build_seasonal_naive_forecast(
        history,
        periods,
        forecast_name="naive_w1",
        horizon_periods=168,
        inputs=["trips.csv"],
        root=tmp_path,
    )
    assert folder == tmp_path / "naive_w1"
    assert list_forecasts(tmp_path) == ["naive_w1"]

    demand, meta = load_forecast("naive_w1", tmp_path)
    assert meta.model_name == "seasonal_naive"
    assert meta.horizon_periods == 168
    # The horizon starts right after the history ends.
    assert pd.Timestamp(meta.t0) == MONDAY + pd.Timedelta(days=7)
    grid = forecast_periods_from_meta(meta)
    assert len(grid) == 168
    # One week of history: the hour-of-week mean is the single observation.
    assert demand.set_index("period_id")["quantity"].to_dict() == {8: 2}


# ---------------------------------------------------------------------------
# apply_forecast_demand and the golden path
# ---------------------------------------------------------------------------
def synthetic_history_resolved():
    """Build a week-long synthetic scenario: 3 trips at period 8, 2 at period 9."""
    trips = [
        ("s1", "s2", 8, 8),
        ("s1", "s2", 8, 9),
        ("s2", "s1", 8, 9),
        ("s1", "s2", 9, 10),
        ("s2", "s3", 9, 9),
    ]
    resolved = scenarios.build_resolved(trips, n_periods=168)
    # build_resolved's grid starts 2026-01-01; apply_forecast_demand checks
    # the period length against the scenario's, so state it on the fixture.
    resolved.period_len = pd.Timedelta(hours=1)
    return resolved


def forecast_tables(resolved):
    """Build and reload a seasonal naive forecast for the scenario's next week."""
    horizon = hourly_periods(resolved.periods_df["end_timestamp"].iloc[-1], 168)
    fractional = seasonal_naive_fractional(
        resolved.historical_demand_df, resolved.periods_df, horizon
    )
    return round_forecast_demand(fractional), horizon


def test_apply_forecast_demand_replaces_tables_and_keeps_the_original():
    resolved = synthetic_history_resolved()
    demand, horizon = forecast_tables(resolved)
    before = resolved.historical_demand_df

    out = apply_forecast_demand(resolved, demand, horizon)

    assert out is not resolved
    assert out.historical_demand_df is demand
    assert out.periods_df is horizon
    assert out.t0 == horizon["start_timestamp"].iloc[0]
    assert set(out.historical_od_matrix_df["period_id"]) <= set(horizon["period_id"])
    # The original container is untouched (the Run page shares one cached copy).
    assert resolved.historical_demand_df is before
    assert resolved.periods_df is not horizon


def test_apply_forecast_demand_rejects_demand_without_od_rows():
    resolved = synthetic_history_resolved()
    demand, horizon = forecast_tables(resolved)
    # Demand at an hour of week no historical trip ever departed at: the OD
    # mapping has nothing for it, so the engine would silently drop it.
    orphan = demand_table([(100, "s1", 1)])
    orphan["quantity"] = orphan["quantity"].astype("int64")
    with pytest.raises(ValueError, match="no OD rows"):
        apply_forecast_demand(resolved, pd.concat([demand, orphan], ignore_index=True), horizon)


def test_apply_forecast_demand_rejects_unknown_facilities():
    resolved = synthetic_history_resolved()
    demand, horizon = forecast_tables(resolved)
    stranger = demand.assign(facility_id="s999")
    with pytest.raises(ValueError, match="unknown facilities"):
        apply_forecast_demand(resolved, stranger, horizon)


def test_apply_forecast_demand_rejects_a_different_period_length():
    resolved = synthetic_history_resolved()
    demand, _ = forecast_tables(resolved)
    two_hour_grid = get_forecast_periods_df(
        resolved.periods_df["end_timestamp"].iloc[-1], 168, pd.Timedelta(hours=2)
    )
    with pytest.raises(ValueError, match="period length"):
        apply_forecast_demand(resolved, demand, two_hour_grid)


def test_apply_saved_forecast_runs_the_scenario_on_the_artifact(tmp_path):
    """The named-forecast step: a saved artifact in, a scenario running on it out."""
    resolved = synthetic_history_resolved()
    forecast.build_seasonal_naive_forecast(
        resolved.historical_demand_df,
        resolved.periods_df,
        forecast_name="naive_w1",
        horizon_periods=168,
        inputs=["trips.csv"],
        root=tmp_path,
    )
    saved_demand, _ = load_forecast("naive_w1", tmp_path)

    out, dropped_share = apply_saved_forecast(resolved, "naive_w1", root=tmp_path)

    # The forecast came from the scenario's own history, so nothing is cut.
    assert dropped_share == 0.0
    assert out is not resolved
    assert out.historical_demand_df["quantity"].sum() == saved_demand["quantity"].sum()
    assert len(out.periods_df) == 168
    assert out.t0 == out.periods_df["start_timestamp"].iloc[0]
    # The horizon starts right where the history ends.
    assert out.t0 == resolved.periods_df["end_timestamp"].iloc[-1]


def test_apply_saved_forecast_cuts_rows_the_scenario_cannot_run(tmp_path):
    """A demand row at an uncovered station-hour is cut, not a ``ValueError``.

    ``apply_forecast_demand`` refuses demand without OD rows; the
    named-forecast step cuts those rows first and reports their share, so a
    forecast built from wider data than the scenario's trip CSV still runs.
    """
    resolved = synthetic_history_resolved()
    demand, horizon = forecast_tables(resolved)
    # Demand at an hour of week no historical trip ever departed at.
    orphan = demand_table([(100, "s1", 1)])
    orphan["quantity"] = orphan["quantity"].astype("int64")
    total = int(demand["quantity"].sum()) + 1
    meta = ForecastMeta(
        forecast_name="wide",
        model_name="seasonal_naive",
        model_version="1",
        created_at="2026-01-01T00:00:00",
        t0=pd.Timestamp(horizon["start_timestamp"].iloc[0]).isoformat(),
        horizon_periods=len(horizon),
        period_len_hours=1.0,
        history_start=pd.Timestamp(resolved.periods_df["start_timestamp"].iloc[0]).isoformat(),
        history_end=pd.Timestamp(resolved.periods_df["end_timestamp"].iloc[-1]).isoformat(),
        inputs=["trips.csv"],
    )
    save_forecast(pd.concat([demand, orphan], ignore_index=True), meta, tmp_path)

    out, dropped_share = apply_saved_forecast(resolved, "wide", root=tmp_path)

    assert dropped_share == pytest.approx(1 / total)
    assert out.historical_demand_df["quantity"].sum() == total - 1


def test_forecast_run_departs_exactly_the_forecast_demand():
    """The golden path: sized forecast run, zero violations, demand fully served."""
    from gbp.model import flows_to_departures

    resolved = synthetic_history_resolved()
    demand, horizon = forecast_tables(resolved)
    assert demand["quantity"].sum() > 0, "the fixture must forecast something"

    forecast_resolved = apply_forecast_demand(resolved, demand, horizon)
    run = run_sized_scenario(
        forecast_resolved,
        scenario_id="forecast_test",
        number_of_periods=len(horizon),
    )

    assert run.violations == []
    departed = flows_to_departures(run.simulated_flows_df)
    merged = demand.merge(
        departed,
        on=["period_id", "facility_id", "commodity_category"],
        how="outer",
        suffixes=("_forecast", "_departed"),
    ).fillna(0)
    assert (merged["quantity_forecast"] == merged["quantity_departed"]).all()
