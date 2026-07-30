"""Tests for the raw trip loader and its processed parquet copy.

``load_trips_raw_df`` keeps a processed copy of each CSV in
``data/processed/`` (Notations.md §15). These tests pin the cache contract:
the first load writes the copy, later loads read it, a CSV newer than its
copy forces a rebuild, and a bad CSV is never cached.
"""

import os

import pandas as pd
import pytest

from domains.citybike.loaders.dataloader_raw import load_trips_raw_df, processed_trips_path

CSV_HEADER = (
    "ride_id,rideable_type,started_at,ended_at,"
    "start_station_name,start_station_id,end_station_name,end_station_id,"
    "start_lat,start_lng,end_lat,end_lng,member_casual"
)


def _trip_row(ride_id: str, start_lat: float = 40.7) -> str:
    return (
        f"{ride_id},classic_bike,2026-01-01 00:05:00,2026-01-01 00:20:00,"
        f"A,st_a,B,st_b,{start_lat},-74.0,40.72,-74.01,member"
    )


def _write_csv(tmp_path, rows: list[str]) -> str:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    csv = raw_dir / "trips.csv"
    csv.write_text("\n".join([CSV_HEADER, *rows]) + "\n")
    return str(csv)


def test_first_load_writes_the_processed_copy(tmp_path):
    csv = _write_csv(tmp_path, [_trip_row("r1"), _trip_row("r2")])
    trips = load_trips_raw_df(csv)
    assert len(trips) == 2
    processed = processed_trips_path(csv)
    assert processed == tmp_path / "processed" / "trips.parquet"
    assert processed.exists()
    pd.testing.assert_frame_equal(pd.read_parquet(processed), trips)


def test_second_load_reads_the_processed_copy(tmp_path):
    csv = _write_csv(tmp_path, [_trip_row("r1")])
    load_trips_raw_df(csv)

    # Rewrite the copy with a marker row; a load that returns the marker
    # proves it read the parquet file, not the CSV.
    processed = processed_trips_path(csv)
    marked = pd.read_parquet(processed).assign(ride_id="from_processed_copy")
    marked.to_parquet(processed, index=False)

    trips = load_trips_raw_df(csv)
    assert list(trips["ride_id"]) == ["from_processed_copy"]


def test_csv_newer_than_the_copy_forces_a_rebuild(tmp_path):
    csv = _write_csv(tmp_path, [_trip_row("r1")])
    load_trips_raw_df(csv)
    processed = processed_trips_path(csv)
    marked = pd.read_parquet(processed).assign(ride_id="from_processed_copy")
    marked.to_parquet(processed, index=False)

    # Make the CSV strictly newer than the copy: the copy is stale now.
    newer = processed.stat().st_mtime + 10
    os.utime(csv, (newer, newer))

    trips = load_trips_raw_df(csv)
    assert list(trips["ride_id"]) == ["r1"]
    # The rebuilt copy replaced the marked one.
    assert list(pd.read_parquet(processed)["ride_id"]) == ["r1"]


def test_out_of_area_rows_are_dropped_not_fatal(tmp_path):
    # Most published months carry a few rows at test docks or with plainly
    # wrong coordinates; cleaning drops them like rows with missing fields.
    csv = _write_csv(
        tmp_path,
        [_trip_row("r1"), _trip_row("bad_dock", start_lat=34.02), _trip_row("r2")],
    )
    trips = load_trips_raw_df(csv)
    assert list(trips["ride_id"]) == ["r1", "r2"]


def test_reversed_trip_times_are_dropped_not_fatal(tmp_path):
    # On the fall-back night of daylight saving time the wall-clock
    # timestamps repeat one hour, so a trip riding across the change looks
    # like it ends before it starts. Cleaning drops such rows.
    reversed_row = (
        "dst,classic_bike,2025-11-02 01:36:08,2025-11-02 01:03:36,"
        "A,st_a,B,st_b,40.7,-74.0,40.72,-74.01,member"
    )
    csv = _write_csv(tmp_path, [_trip_row("r1"), reversed_row])
    trips = load_trips_raw_df(csv)
    assert list(trips["ride_id"]) == ["r1"]


def test_bad_csv_fails_and_is_not_cached(tmp_path):
    # A file without a required column breaks TRIPS_SCHEMA.
    header = CSV_HEADER.replace("rideable_type,", "")
    row = _trip_row("r1").replace("classic_bike,", "")
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    csv = raw_dir / "trips.csv"
    csv.write_text("\n".join([header, row]) + "\n")
    with pytest.raises(ValueError, match="breaks the trips schema"):
        load_trips_raw_df(str(csv))
    assert not processed_trips_path(str(csv)).exists()
