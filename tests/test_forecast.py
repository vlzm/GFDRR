"""Tests for the forecast path (phase 1 of the demand forecasting plan).

The seasonal naive forecast, the one rounding rule, the forecast artifact
save/load round trip, the hour-of-week OD mapping, and the golden path: a
forecast run on a synthetic scenario finishes with zero invariant violations
and departs exactly the forecast demand.
"""

import pandas as pd
import pytest

from gbp.consumers.simulator import run_sized_scenario
from gbp.loaders.dataloader_graph import (
    apply_forecast_demand,
    get_forecast_periods_df,
    hour_of_week,
    map_od_matrix_by_hour_of_week,
)
from gbp.ml import forecast
from gbp.ml.models import create_model
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
# hour_of_week and the forecast period grid
# ---------------------------------------------------------------------------
def test_hour_of_week_counts_from_monday_midnight():
    ts = pd.Series(
        [
            MONDAY,  # Monday 00:00
            MONDAY + pd.Timedelta(hours=8),  # Monday 08:00
            MONDAY + pd.Timedelta(days=6, hours=23),  # Sunday 23:00
            MONDAY + pd.Timedelta(days=7),  # next Monday 00:00
        ]
    )
    assert hour_of_week(ts).tolist() == [0, 8, 167, 0]


def test_forecast_periods_restart_at_zero():
    grid = hourly_periods(MONDAY, 3)
    assert grid["period_id"].tolist() == [0, 1, 2]
    assert grid["start_timestamp"].iloc[0] == MONDAY
    assert grid["end_timestamp"].iloc[2] == MONDAY + pd.Timedelta(hours=3)


# ---------------------------------------------------------------------------
# Seasonal naive (through the model interface)
# ---------------------------------------------------------------------------
def seasonal_naive_fractional(
    history: pd.DataFrame, periods: pd.DataFrame, horizon: pd.DataFrame
) -> pd.DataFrame:
    """Run the interface path: counts grid -> forecast input -> predict."""
    counts = forecast.counts_from_demand(history, periods)
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


def test_counts_from_demand_rejects_demand_outside_the_grid():
    periods = hourly_periods(MONDAY, 24)
    history = demand_table([(100, "s1", 1)])
    with pytest.raises(ValueError, match="outside the period grid"):
        forecast.counts_from_demand(history, periods)


def test_counts_from_demand_fills_the_grid_with_zeros():
    periods = hourly_periods(MONDAY, 24)
    counts = forecast.counts_from_demand(demand_table([(8, "s1", 2)]), periods)
    assert len(counts) == 24
    assert counts["quantity"].sum() == 2
    assert counts.loc[counts["quantity"] > 0, "start_timestamp"].tolist() == [
        MONDAY + pd.Timedelta(hours=8)
    ]


# ---------------------------------------------------------------------------
# The rounding rule
# ---------------------------------------------------------------------------
def test_round_forecast_demand_preserves_period_commodity_totals():
    fractional = demand_table(
        [(0, "s1", 0.4), (0, "s2", 0.4), (0, "s3", 0.4), (0, "s4", 0.4), (0, "s5", 0.4)]
    )
    out = forecast.round_forecast_demand(fractional)
    # Row-by-row rounding would give 0; the group total 2.0 survives.
    assert out["quantity"].sum() == 2
    assert (out["quantity"] >= 1).all()


def test_round_forecast_demand_never_invents_demand():
    fractional = demand_table([(0, "s1", 2.0), (0, "s2", 0.0), (0, "s3", 0.9)])
    out = forecast.round_forecast_demand(fractional)
    # s2 has fractional part zero, so it can never be rounded up; zero rows
    # are dropped from the table.
    assert set(out["facility_id"]) == {"s1", "s3"}
    assert out.set_index("facility_id")["quantity"].to_dict() == {"s1": 2, "s3": 1}


def test_round_forecast_demand_breaks_ties_by_facility_id():
    fractional = demand_table([(0, "s2", 0.5), (0, "s1", 0.5)])
    out = forecast.round_forecast_demand(fractional)
    # Total 1.0: one bike to hand out, equal remainders — s1 wins the tie.
    assert out["facility_id"].tolist() == ["s1"]
    assert out["quantity"].tolist() == [1]


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
    assert forecast.list_forecasts(tmp_path) == ["naive_w1"]

    demand, meta = forecast.load_forecast("naive_w1", tmp_path)
    assert meta.model_name == "seasonal_naive"
    assert meta.horizon_periods == 168
    # The horizon starts right after the history ends.
    assert pd.Timestamp(meta.t0) == MONDAY + pd.Timedelta(days=7)
    grid = forecast.forecast_periods_from_meta(meta)
    assert len(grid) == 168
    # One week of history: the hour-of-week mean is the single observation.
    assert demand.set_index("period_id")["quantity"].to_dict() == {8: 2}


def test_load_forecast_unknown_name(tmp_path):
    with pytest.raises(FileNotFoundError, match="unknown forecast"):
        forecast.load_forecast("missing", tmp_path)


# ---------------------------------------------------------------------------
# OD mapping by hour of week
# ---------------------------------------------------------------------------
def test_map_od_matrix_pools_periods_with_one_hour_of_week():
    periods = hourly_periods(MONDAY, 2 * 168)
    # Monday 08:00 in two different weeks: 3 trips s1->s2 (1 period long) and
    # 1 trip s1->s3 (2 periods long) in week one; 2 trips s1->s3 in week two.
    od = pd.DataFrame(
        {
            "source_id": ["s1", "s1", "s1"],
            "planned_target_id": ["s2", "s3", "s3"],
            "period_id": [8, 8, 168 + 8],
            "commodity_category": CLASSIC,
            "count": [3, 1, 2],
            "duration": [1, 2, 5],
            "probability": [0.75, 0.25, 1.0],
        }
    )
    horizon = hourly_periods(MONDAY + pd.Timedelta(days=14), 168)

    out = map_od_matrix_by_hour_of_week(od, periods, horizon)

    # Every mapped row lands on the forecast period with the same hour of week.
    assert set(out["period_id"]) == {8}
    row = out.set_index("planned_target_id")
    # Counts pool across the weeks; the shares are recomputed from the pool.
    assert row.loc["s2", "count"] == 3
    assert row.loc["s3", "count"] == 3
    assert row.loc["s2", "probability"] == pytest.approx(0.5)
    assert row.loc["s3", "probability"] == pytest.approx(0.5)
    # Duration is the count-weighted mean: (1*2 + 2*5) / 3 = 4.
    assert row.loc["s3", "duration"] == 4


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
    return forecast.round_forecast_demand(fractional), horizon


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
