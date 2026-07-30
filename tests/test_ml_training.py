"""Tests for the training-table builder (phase 2 of the demand forecasting plan).

The full month grid with zero rows kept, the month filter, agreement with the
simulator's own departures read-model, and the idempotent parquet partition —
now carrying the feature columns and the stockout mark (phase 3).
"""

import numpy as np
import pandas as pd
import pytest

from domains.citybike.loaders.dataloader_graph import get_historical_flows_df
from domains.citybike.ml import training
from domains.citybike.ml.features import FEATURE_COLUMNS, HISTORY_FEATURES
from gbp.model import flows_to_departures

CLASSIC = "classic_bike"
ELECTRIC = "electric_bike"

#: February 2025: the shortest month, 28 days = 672 hours.
FEB = pd.Timestamp("2025-02-01")
FEB_HOURS = 28 * 24


def trips_df(rows):
    """Trips from ``(started_at, start_id, end_id, rideable_type)`` tuples."""
    return pd.DataFrame(
        {
            "started_at": pd.to_datetime([r[0] for r in rows]),
            "ended_at": pd.to_datetime([r[0] for r in rows]) + pd.Timedelta(minutes=10),
            "start_station_id": pd.array([r[1] for r in rows], dtype="string"),
            "end_station_id": pd.array([r[2] for r in rows], dtype="string"),
            "rideable_type": pd.array([r[3] for r in rows], dtype="string"),
        }
    )


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


# ---------------------------------------------------------------------------
# departure_counts
# ---------------------------------------------------------------------------
def test_departure_counts_covers_the_full_month_with_zero_rows():
    trips = trips_df(
        [
            ("2025-02-01 05:10", "s1", "s2", CLASSIC),
            ("2025-02-01 05:40", "s1", "s2", CLASSIC),
            ("2025-02-15 12:00", "s2", "s1", ELECTRIC),
        ]
    )
    table = training.departure_counts(trips, "202502")

    # Full grid: every hour of February x 2 stations x 2 bike types.
    assert len(table) == FEB_HOURS * 2 * 2
    assert table["quantity"].sum() == 3
    by_key = table.set_index(["period_id", "facility_id", "commodity_category"])["quantity"]
    assert by_key[(5, "s1", CLASSIC)] == 2
    assert by_key[(14 * 24 + 12, "s2", ELECTRIC)] == 1
    # A station-hour with no departures is a real zero observation.
    assert by_key[(5, "s2", CLASSIC)] == 0
    # Each period carries its wall-clock hour.
    assert table["start_timestamp"].iloc[0] == FEB
    assert table["start_timestamp"].max() == FEB + pd.Timedelta(hours=FEB_HOURS - 1)


def test_departure_counts_gives_zero_rows_to_a_station_that_only_receives():
    trips = trips_df([("2025-02-01 05:10", "s1", "s9", CLASSIC)])
    table = training.departure_counts(trips, "202502")
    s9 = table[table["facility_id"] == "s9"]
    assert len(s9) == FEB_HOURS
    assert (s9["quantity"] == 0).all()


def test_departure_counts_keeps_neighbor_months_out():
    trips = trips_df(
        [
            ("2025-01-31 23:50", "s1", "s2", CLASSIC),  # January straggler in the file
            ("2025-02-01 00:10", "s1", "s2", CLASSIC),
            ("2025-03-01 00:00", "s1", "s2", CLASSIC),  # first hour of March
        ]
    )
    table = training.departure_counts(trips, "202502")
    assert table["quantity"].sum() == 1


def test_departure_counts_rejects_a_month_with_no_trips():
    trips = trips_df([("2025-02-01 05:10", "s1", "s2", CLASSIC)])
    with pytest.raises(ValueError, match="no trips start inside 202512"):
        training.departure_counts(trips, "202512")


def test_departure_counts_agrees_with_the_flows_read_model():
    """Counting trips by start hour == counting departed events in the journal."""
    trips = trips_df(
        [
            ("2025-02-01 05:10", "s1", "s2", CLASSIC),
            ("2025-02-01 05:40", "s1", "s2", CLASSIC),
            ("2025-02-01 05:55", "s2", "s1", ELECTRIC),
            ("2025-02-20 17:00", "s2", "s3", CLASSIC),
        ]
    )
    table = training.departure_counts(trips, "202502")

    flows = get_historical_flows_df(trips, FEB, pd.Timedelta(hours=1))
    departed = flows_to_departures(flows)

    nonzero = table[table["quantity"] > 0][
        ["period_id", "facility_id", "commodity_category", "quantity"]
    ].reset_index(drop=True)
    departed = departed.sort_values(nonzero.columns[:3].tolist()).reset_index(drop=True)
    pd.testing.assert_frame_equal(nonzero, departed, check_dtype=False, check_categorical=False)


# ---------------------------------------------------------------------------
# The parquet partition
# ---------------------------------------------------------------------------
@pytest.fixture
def raw(tmp_path):
    folder = tmp_path / "raw"
    folder.mkdir()
    rows = [
        new_row("2025-02-01 05:10:00", "s1", "s2"),
        new_row("2025-02-01 05:40:00", "s1", "s2"),
        new_row("2025-02-15 12:00:00", "s2", "s1", ELECTRIC),
    ]
    (folder / "202502-citibike-tripdata_1.csv").write_text("\n".join([NEW_HEADER, *rows]) + "\n")
    return folder


@pytest.fixture
def weather():
    dates = pd.date_range("2025-02-01", "2025-02-28", freq="D")
    return pd.DataFrame(
        {
            "date": dates,
            "temperature_max_c": np.full(len(dates), 4.0),
            "temperature_min_c": np.full(len(dates), -3.0),
            "precipitation_mm": np.full(len(dates), 0.5),
        }
    )


def test_write_month_partition_builds_the_checked_parquet(raw, weather, tmp_path):
    root = tmp_path / "training"
    path = training.write_month_partition("202502", raw=raw, root=root, weather_df=weather)
    assert path == root / "202502.parquet"

    table = pd.read_parquet(path)
    assert len(table) == FEB_HOURS * 2 * 2
    assert table["quantity"].sum() == 3
    # The feature columns are on board; the oldest month has no earlier
    # partitions, so its history features stay NaN.
    assert set(FEATURE_COLUMNS) < set(table.columns)
    assert table["temperature_max_c"].eq(4.0).all()
    assert table[HISTORY_FEATURES].isna().all().all()
    # No status dump on disk -> the mark is unknown, not zero.
    assert table["stockout_share"].isna().all()


def test_write_month_partition_is_idempotent(raw, weather, tmp_path):
    root = tmp_path / "training"
    path = training.write_month_partition("202502", raw=raw, root=root, weather_df=weather)
    first = path.read_bytes()
    again = training.write_month_partition("202502", raw=raw, root=root, weather_df=weather)
    assert again.read_bytes() == first


def test_build_month_partition_needs_the_raw_files(tmp_path):
    with pytest.raises(FileNotFoundError, match="no raw CSVs for 202502"):
        training.build_month_partition("202502", raw=tmp_path / "empty")


def test_partition_marks_stockout_hours_for_covered_facilities(raw, weather, tmp_path):
    # The feed knows one station at the trips' coordinates: empty for the
    # first half of local hour 5 on Feb 1 (10:00 UTC), stocked afterwards.
    records = pd.DataFrame(
        {
            "nuid": ["a", "a"],
            "latitude": [40.75, 40.75],  # s1 sits at (40.75, -73.99) in the fixture
            "longitude": [-73.99, -73.99],
            "bikes": [0, 2],
            "timestamp": pd.to_datetime(["2025-02-01 10:00", "2025-02-01 10:30"]),
        }
    )
    table = training.build_month_partition(
        "202502", raw=raw, root=tmp_path / "training", weather_df=weather, status_records_df=records
    )

    by_key = table.set_index(["period_id", "facility_id", "commodity_category"])["stockout_share"]
    assert by_key[(5, "s1", CLASSIC)] == 0.5
    assert by_key[(5, "s1", ELECTRIC)] == 0.5  # the mark is per station, not per bike type
    assert by_key[(6, "s1", CLASSIC)] == 0.0  # covered and stocked -> zero, not unknown
    # s2 sits at (40.76, -73.97), too far from the feed station -> unknown.
    assert table[table["facility_id"] == "s2"]["stockout_share"].isna().all()
