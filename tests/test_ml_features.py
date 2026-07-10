"""Tests for the feature builder and the stockout mark (phase 3 of the plan).

The calendar, weather and history features on tiny fixtures, the leakage
guard, the stockout-share computation from status records, the coordinate
matching, and the phase's closing check: the training table and the
forecast input hold identical feature values for the same station-day.
"""

import numpy as np
import pandas as pd
import pytest

from gbp.loaders.dataloader_graph import get_forecast_periods_df
from gbp.ml import features, forecast, station_status, training

CLASSIC = "classic_bike"
ELECTRIC = "electric_bike"

#: 2025-02-03 is a Monday; timestamps below build on it for readable lags.
MONDAY = pd.Timestamp("2025-02-03")
WEEK = pd.Timedelta(weeks=1)


def counts_df(rows):
    """Departure counts from ``(start_timestamp, facility_id, quantity)`` tuples."""
    return pd.DataFrame(
        {
            "start_timestamp": pd.to_datetime([r[0] for r in rows]),
            "facility_id": [r[1] for r in rows],
            "commodity_category": CLASSIC,
            "quantity": [r[2] for r in rows],
        }
    )


def weather_df(start, end):
    """Daily weather over ``[start, end]`` with values derived from the date."""
    dates = pd.date_range(start, end, freq="D")
    return pd.DataFrame(
        {
            "date": dates,
            "temperature_max_c": np.arange(len(dates), dtype="float64"),
            "temperature_min_c": np.arange(len(dates), dtype="float64") - 8.0,
            "precipitation_mm": (np.arange(len(dates)) % 3) * 1.5,
        }
    )


# ---------------------------------------------------------------------------
# Calendar features
# ---------------------------------------------------------------------------
def test_calendar_features_come_from_the_timestamp_alone():
    table = counts_df(
        [
            ("2025-07-04 17:00", "s1", 0),  # Friday, Independence Day
            ("2025-02-03 05:00", "s1", 0),  # Monday, a plain day
        ]
    )
    out = features.add_calendar_features(table)
    assert out["hour_of_day"].tolist() == [17, 5]
    assert out["day_of_week"].tolist() == [4, 0]
    assert out["month"].tolist() == [7, 2]
    assert out["is_holiday"].tolist() == [True, False]


# ---------------------------------------------------------------------------
# Weather features
# ---------------------------------------------------------------------------
def test_weather_joins_by_calendar_date():
    table = counts_df([("2025-02-03 05:00", "s1", 0), ("2025-02-03 23:00", "s1", 0)])
    weather = weather_df("2025-02-03", "2025-02-03")
    out = features.add_weather_features(table, weather)
    # Every hour of the day carries the same daily values.
    assert out["temperature_max_c"].tolist() == [0.0, 0.0]
    assert out["temperature_min_c"].tolist() == [-8.0, -8.0]


def test_weather_missing_dates_stay_nan():
    table = counts_df([("2025-02-04 05:00", "s1", 0)])
    weather = weather_df("2025-02-03", "2025-02-03")
    out = features.add_weather_features(table, weather)
    assert out["temperature_max_c"].isna().all()


def test_weather_rejects_two_rows_for_one_date():
    table = counts_df([("2025-02-03 05:00", "s1", 0)])
    weather = pd.concat([weather_df("2025-02-03", "2025-02-03")] * 2, ignore_index=True)
    with pytest.raises(ValueError, match="one row per date"):
        features.add_weather_features(table, weather)


# ---------------------------------------------------------------------------
# History features
# ---------------------------------------------------------------------------
def test_lag_1w_is_the_count_at_the_same_hour_one_week_earlier():
    history = counts_df([(MONDAY + pd.Timedelta(hours=8), "s1", 5)])
    targets = counts_df(
        [
            (MONDAY + WEEK + pd.Timedelta(hours=8), "s1", 0),
            (MONDAY + WEEK + pd.Timedelta(hours=9), "s1", 0),  # no observation an hour later
        ]
    )
    out = features.add_history_features(targets, history)
    assert out["quantity_lag_1w"].tolist()[0] == 5.0
    assert pd.isna(out["quantity_lag_1w"].iloc[1])


def test_mean_4w_averages_only_the_weeks_the_history_covers():
    # Two same-hour observations, one and two weeks before the target.
    history = counts_df(
        [
            (MONDAY + pd.Timedelta(hours=8), "s1", 3),
            (MONDAY + WEEK + pd.Timedelta(hours=8), "s1", 5),
        ]
    )
    targets = counts_df([(MONDAY + 2 * WEEK + pd.Timedelta(hours=8), "s1", 0)])
    out = features.add_history_features(targets, history)
    assert out["quantity_mean_4w"].iloc[0] == 4.0


def test_facility_means_come_from_the_history_only():
    history = counts_df(
        [
            (MONDAY + pd.Timedelta(hours=8), "s1", 2),
            (MONDAY + pd.Timedelta(hours=9), "s1", 4),
            (MONDAY + WEEK + pd.Timedelta(hours=8), "s1", 6),
        ]
    )
    targets = counts_df(
        [
            (MONDAY + 2 * WEEK + pd.Timedelta(hours=8), "s1", 100),
            (MONDAY + 2 * WEEK + pd.Timedelta(hours=8), "s9", 100),  # never seen in history
        ]
    )
    out = features.add_history_features(targets, history)
    assert out["facility_mean"].iloc[0] == 4.0  # (2 + 4 + 6) / 3
    assert out["facility_hour_of_week_mean"].iloc[0] == 4.0  # Monday 08:00: (2 + 6) / 2
    assert (
        out.loc[1, ["quantity_lag_1w", "facility_mean", "facility_hour_of_week_mean"]].isna().all()
    )


def test_history_overlapping_the_targets_is_rejected():
    table = counts_df([(MONDAY, "s1", 1)])
    with pytest.raises(ValueError, match="strictly before"):
        features.build_features(table, table)


def test_history_rejects_duplicate_station_hours():
    history = counts_df([(MONDAY, "s1", 1), (MONDAY, "s1", 2)])
    targets = counts_df([(MONDAY + WEEK, "s1", 0)])
    with pytest.raises(ValueError, match="more than one count"):
        features.add_history_features(targets, history)


def test_clip_history_window_keeps_the_last_weeks_only():
    history = counts_df(
        [
            (MONDAY - (features.HISTORY_WEEKS + 1) * WEEK, "s1", 9),  # too old
            (MONDAY - WEEK, "s1", 1),
        ]
    )
    clipped = features.clip_history_window(history, MONDAY)
    assert clipped["quantity"].tolist() == [1]


def test_build_features_without_weather_keeps_the_columns_nan():
    targets = counts_df([(MONDAY + WEEK, "s1", 0)])
    history = counts_df([(MONDAY, "s1", 1)])
    out = features.build_features(targets, history)
    assert list(out.columns[-len(features.FEATURE_COLUMNS) :]) == features.FEATURE_COLUMNS
    assert out[features.WEATHER_FEATURES].isna().all().all()


def test_feature_column_lists_agree():
    combined = features.CALENDAR_FEATURES + features.WEATHER_FEATURES + features.HISTORY_FEATURES
    assert features.FEATURE_COLUMNS == combined


# ---------------------------------------------------------------------------
# The stockout mark (station-status records)
# ---------------------------------------------------------------------------
def status_records(rows):
    """Feed records from ``(timestamp_utc, nuid, bikes)`` tuples at one place."""
    return pd.DataFrame(
        {
            "nuid": [r[1] for r in rows],
            "latitude": 40.75,
            "longitude": -73.99,
            "bikes": [r[2] for r in rows],
            "timestamp": pd.to_datetime([r[0] for r in rows]),
        }
    )


def test_stockout_shares_convert_utc_and_carry_the_last_record_forward():
    # Local time is UTC-5 in February: 05:00 UTC on Feb 1 is local midnight.
    records = status_records(
        [
            ("2025-02-01 05:00", "a", 0),  # empty from local 00:00
            ("2025-02-01 05:30", "a", 2),  # bikes back at local 00:30
            ("2025-02-01 06:45", "a", 0),  # empty from local 01:45, no later record
            ("2025-02-01 05:00", "b", 3),  # never empty
        ]
    )
    shares = station_status.stockout_shares(records, "202502")
    by_hour = shares.set_index(["nuid", "start_timestamp"])["stockout_share"]

    assert by_hour[("a", pd.Timestamp("2025-02-01 00:00"))] == 0.5
    assert by_hour[("a", pd.Timestamp("2025-02-01 01:00"))] == 0.25
    # The last record holds until the month ends (no record = no change).
    assert by_hour[("a", pd.Timestamp("2025-02-01 02:00"))] == 1.0
    assert by_hour[("a", pd.Timestamp("2025-02-28 23:00"))] == 1.0
    assert "b" not in shares["nuid"].tolist()


def test_stockout_shares_clip_an_interval_that_starts_before_the_month():
    records = status_records(
        [
            ("2025-01-31 20:00", "a", 0),  # empty since January, local 15:00
            ("2025-02-01 05:30", "a", 1),  # refilled at local 00:30 on Feb 1
        ]
    )
    shares = station_status.stockout_shares(records, "202502")
    assert len(shares) == 1
    assert shares["start_timestamp"].iloc[0] == pd.Timestamp("2025-02-01 00:00")
    assert shares["stockout_share"].iloc[0] == 0.5


def test_match_facilities_uses_the_distance_cutoff():
    facilities = pd.DataFrame(
        {
            "facility_id": ["near", "far"],
            "latitude": [40.7500, 40.7600],
            "longitude": [-73.9900, -73.9900],
        }
    )
    stations = pd.DataFrame(
        {
            "nuid": ["a"],
            "latitude": [40.75001],  # about a meter from "near"
            "longitude": [-73.99],
        }
    )
    matched = station_status.match_facilities(facilities, stations)
    assert matched.to_dict("records") == [{"facility_id": "near", "nuid": "a"}]


# ---------------------------------------------------------------------------
# The phase's closing check: training table == forecast input
# ---------------------------------------------------------------------------
NEW_HEADER = (
    "ride_id,rideable_type,started_at,ended_at,start_station_name,start_station_id,"
    "end_station_name,end_station_id,start_lat,start_lng,end_lat,end_lng,member_casual"
)


def new_row(started_at, start_id, end_id, rideable_type=CLASSIC):
    ended_at = pd.Timestamp(started_at) + pd.Timedelta(minutes=10)
    return (
        f"R1,{rideable_type},{started_at},{ended_at},A,{start_id},"
        f"B,{end_id},40.75,-73.99,40.76,-73.97,member"
    )


def test_training_table_and_forecast_input_agree_for_the_same_station_day(tmp_path):
    """Phase 3 done-when: identical feature values for the same station-day."""
    raw = tmp_path / "raw"
    raw.mkdir()
    root = tmp_path / "training"
    weather = weather_df("2025-02-01", "2025-03-31")

    # February trips: repeating Monday/Saturday hours give the March rows
    # real lags and hour-of-week means. March trips only shape March's grid.
    feb_rows = [
        new_row(f"2025-02-{day:02d} 08:15:00", "s1", "s2")
        for day in (3, 10, 17, 24)  # the Mondays of February 2025
    ] + [new_row("2025-02-08 12:00:00", "s2", "s1"), new_row("2025-02-22 12:30:00", "s2", "s1")]
    mar_rows = [
        new_row("2025-03-03 08:20:00", "s1", "s2"),
        new_row("2025-03-15 12:00:00", "s2", "s1"),
    ]
    (raw / "202502-citibike-tripdata_1.csv").write_text("\n".join([NEW_HEADER, *feb_rows]) + "\n")
    (raw / "202503-citibike-tripdata_1.csv").write_text("\n".join([NEW_HEADER, *mar_rows]) + "\n")

    training.write_month_partition("202502", raw=raw, root=root, weather_df=weather)
    training.write_month_partition("202503", raw=raw, root=root, weather_df=weather)
    march = pd.read_parquet(training.partition_path("202503", root))

    history = pd.read_parquet(
        training.partition_path("202502", root),
        columns=["start_timestamp", "facility_id", "commodity_category", "quantity"],
    )
    horizon = get_forecast_periods_df(pd.Timestamp("2025-03-01"), 31 * 24, pd.Timedelta(hours=1))
    predicted_from = forecast.forecast_input(history, horizon, weather)

    keys = ["period_id", "facility_id", "commodity_category"]
    merged = march.merge(predicted_from, on=keys, suffixes=("_train", "_forecast"), how="inner")
    # Both paths cover the same grid: every hour of March for both stations.
    assert len(merged) == len(march) == len(predicted_from) == 31 * 24 * 2

    for column in features.FEATURE_COLUMNS:
        pd.testing.assert_series_equal(
            merged[f"{column}_train"],
            merged[f"{column}_forecast"],
            check_names=False,
        )
    # The comparison is not trivially all-NaN: week one of March has real lags,
    # and every row has a facility mean from February.
    assert merged["quantity_lag_1w_train"].notna().any()
    assert merged["facility_mean_train"].notna().all()
