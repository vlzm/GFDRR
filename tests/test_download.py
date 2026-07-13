"""Tests for the trip downloader (``gbp/loaders/download.py``).

The schema harmonization (old and new published files both land in the one
trips schema), the month naming helpers, and the downloader's skip and
extraction logic — the network itself is stubbed out.
"""

import io
import zipfile

import pandas as pd
import pytest

from gbp.loaders import download
from gbp.loaders.dataloader_raw import load_trips_raw_df

NEW_HEADER = (
    "ride_id,rideable_type,started_at,ended_at,start_station_name,start_station_id,"
    "end_station_name,end_station_id,start_lat,start_lng,end_lat,end_lng,member_casual"
)
NEW_ROW = (
    "ABC123,electric_bike,2026-01-02 05:36:24,2026-01-02 05:42:21,"
    "W 42 St,6602.05,E 58 St,6839.04,40.7575,-73.9909,40.7630,-73.9720,member"
)

OLD_HEADER = (
    '"tripduration","starttime","stoptime","start station id","start station name",'
    '"start station latitude","start station longitude","end station id",'
    '"end station name","end station latitude","end station longitude",'
    '"bikeid","usertype","birth year","gender"'
)


def old_row(
    start_id="519",
    end_id="212",
    start_lat="40.7521",
    start_lng="-73.9779",
    usertype="Subscriber",
):
    return (
        f'"360","2019-06-01 00:00:05","2019-06-01 00:06:05",{start_id},"Pershing Sq",'
        f'{start_lat},{start_lng},{end_id},"W 16 St",40.7383,-74.0004,'
        f'"31956",{usertype},"1988","1"'
    )


@pytest.fixture
def raw(tmp_path):
    """Make a data/raw-like folder; the processed cache lands next to it."""
    folder = tmp_path / "raw"
    folder.mkdir()
    return folder


def write_csv(folder, name, lines):
    path = folder / name
    path.write_text("\n".join(lines) + "\n")
    return path


# ---------------------------------------------------------------------------
# Month naming
# ---------------------------------------------------------------------------
def test_normalize_month_accepts_both_forms():
    assert download.normalize_month("202502") == "202502"
    assert download.normalize_month("2025-02") == "202502"


@pytest.mark.parametrize("bad", ["2025", "202513", "202500", "jan-2025"])
def test_normalize_month_rejects_non_months(bad):
    with pytest.raises(ValueError, match="not a month"):
        download.normalize_month(bad)


def test_month_csvs_matches_city_and_jersey_city_files(raw):
    for name in (
        "202601-citibike-tripdata_1.csv",
        "202601-citibike-tripdata_2.csv",
        "JC-202601-citibike-tripdata.csv",
        "202602-citibike-tripdata_1.csv",
    ):
        write_csv(raw, name, [NEW_HEADER])
    found = download.month_csvs("2026-01", raw)
    assert [p.name for p in found] == [
        "202601-citibike-tripdata_1.csv",
        "202601-citibike-tripdata_2.csv",
        "JC-202601-citibike-tripdata.csv",
    ]


# ---------------------------------------------------------------------------
# Schema harmonization
# ---------------------------------------------------------------------------
def test_old_schema_maps_to_the_trips_schema(raw):
    path = write_csv(raw, "201906-citibike-tripdata_1.csv", [OLD_HEADER, old_row()])
    trips = download.load_trips_any_schema(str(path))

    row = trips.iloc[0]
    assert row["started_at"] == pd.Timestamp("2019-06-01 00:00:05")
    assert row["ended_at"] == pd.Timestamp("2019-06-01 00:06:05")
    # Old integer station ids become strings, not floats.
    assert row["start_station_id"] == "519"
    assert row["end_station_id"] == "212"
    # The columns the old files did not have are filled, not invented.
    assert pd.isna(row["ride_id"])
    assert row["rideable_type"] == "classic_bike"
    assert row["member_casual"] == "member"


def test_old_schema_maps_usertype_customer_to_casual(raw):
    path = write_csv(
        raw, "201906-citibike-tripdata_1.csv", [OLD_HEADER, old_row(usertype="Customer")]
    )
    trips = download.load_trips_any_schema(str(path))
    assert trips["member_casual"].tolist() == ["casual"]


def test_old_schema_drops_incomplete_and_out_of_area_rows(raw):
    path = write_csv(
        raw,
        "201906-citibike-tripdata_1.csv",
        [
            OLD_HEADER,
            old_row(),
            old_row(end_id="NULL"),  # missing key field
            old_row(start_lat="0.0", start_lng="0.0"),  # test dock far outside the box
        ],
    )
    trips = download.load_trips_any_schema(str(path))
    assert len(trips) == 1


def test_title_case_old_header_maps_too(raw):
    header = OLD_HEADER.replace("starttime", "Start Time").replace("stoptime", "Stop Time")
    path = write_csv(raw, "201703-citibike-tripdata_1.csv", [header, old_row()])
    trips = download.load_trips_any_schema(str(path))
    assert trips["started_at"].iloc[0] == pd.Timestamp("2019-06-01 00:00:05")


def test_new_schema_goes_through_the_raw_loader_unchanged(raw):
    path = write_csv(raw, "202601-citibike-tripdata_1.csv", [NEW_HEADER, NEW_ROW])
    via_any = download.load_trips_any_schema(str(path))
    direct = load_trips_raw_df(str(path))
    pd.testing.assert_frame_equal(via_any, direct)


def test_old_schema_load_is_cached_in_processed(raw, tmp_path):
    path = write_csv(raw, "201906-citibike-tripdata_1.csv", [OLD_HEADER, old_row()])
    first = download.load_trips_any_schema(str(path))
    assert (tmp_path / "processed" / "201906-citibike-tripdata_1.parquet").exists()
    again = download.load_trips_any_schema(str(path))
    pd.testing.assert_frame_equal(first, again)


# ---------------------------------------------------------------------------
# The downloader (network stubbed out)
# ---------------------------------------------------------------------------
def test_month_zip_keys_picks_city_and_jersey_city(monkeypatch):
    listing = {
        "202601-citibike-tripdata": ["202601-citibike-tripdata.zip"],
        "JC-202601-citibike-tripdata": ["JC-202601-citibike-tripdata.zip"],
    }
    monkeypatch.setattr(download, "_list_bucket_keys", lambda prefix: listing.get(prefix, []))
    assert download.month_zip_keys("202601") == [
        "202601-citibike-tripdata.zip",
        "JC-202601-citibike-tripdata.zip",
    ]


def test_month_zip_keys_names_the_yearly_bundle_when_monthly_is_gone(monkeypatch):
    monkeypatch.setattr(download, "_list_bucket_keys", lambda prefix: [])
    with pytest.raises(ValueError, match="2019-citibike-tripdata.zip"):
        download.month_zip_keys("201906")


def test_download_months_skips_a_month_already_on_disk(raw, monkeypatch):
    write_csv(raw, "202601-citibike-tripdata_1.csv", [NEW_HEADER, NEW_ROW])

    def no_network(*args, **kwargs):
        raise AssertionError("the network must not be touched for a present month")

    monkeypatch.setattr(download, "_list_bucket_keys", no_network)
    monkeypatch.setattr(download, "_download", no_network)
    assert download.download_months(["202601"], raw) == []


def test_download_months_extracts_only_flat_csvs(raw, monkeypatch):
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w") as archive:
        archive.writestr("202401-citibike-tripdata_1.csv", NEW_HEADER + "\n")
        archive.writestr("split/202401-citibike-tripdata_2.csv", NEW_HEADER + "\n")
        archive.writestr("__MACOSX/._202401-citibike-tripdata_1.csv", "junk")
        archive.writestr("readme.txt", "junk")

    monkeypatch.setattr(
        download,
        "_list_bucket_keys",
        lambda prefix: [] if prefix.startswith("JC-") else ["202401-citibike-tripdata.zip"],
    )
    monkeypatch.setattr(
        download, "_download", lambda url, dest: dest.write_bytes(payload.getvalue())
    )
    extracted = download.download_months(["202401"], raw)

    assert sorted(p.name for p in extracted) == [
        "202401-citibike-tripdata_1.csv",
        "202401-citibike-tripdata_2.csv",
    ]
    # The zip itself is gone; only the CSVs stay, flattened out of subfolders.
    assert sorted(p.name for p in raw.iterdir()) == [
        "202401-citibike-tripdata_1.csv",
        "202401-citibike-tripdata_2.csv",
    ]
