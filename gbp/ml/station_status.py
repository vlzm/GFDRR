"""Archived station-status snapshots and the ``stockout_share`` mark.

Censored demand: the training target is the observed departure count, and
the platform assumes observed departures ≈ demand. The assumption fails
exactly where the simulator's own losses live — an hour a station stood
with no bikes records zero departures no matter how many people wanted one.
So each training row carries the mark ``stockout_share``: the share of its
hour (0..1) the station had zero bikes available. Training can exclude or
down-weight rows with a high share. The mark is not a feature — future
stockouts are unknown at prediction time — so the forecast input never has
it.

The source (checked 2026-07-10)
-------------------------------
CityBikes archives the public station feed and publishes monthly dumps:
``https://data.citybik.es/dumps/by-network/<year>/<YYYYMM>-citi-bike-nyc-stats.parquet``,
one row per status change per station: ``nuid`` (the station's feed id),
``name``, ``latitude``, ``longitude``, ``bikes`` (bikes available),
``timestamp``. Dumps for New York start at 2024-11. Earlier public archives
(The Open Bus, the Kaggle station snapshots) stop in 2019-2021, so months
before 2024-11 get no mark: the column stays NaN and the written assumption
above stands for them. Attribution: "Bike-share data by CityBikes
contributors, available from https://data.citybik.es".

How the mark is computed
------------------------
- Dump timestamps are UTC; they are converted to New York wall-clock time
  first. One local month also needs the next month's dump: the last local
  evening of a month lies in the next UTC month. (On the two daylight
  saving switch days the local clock jumps; the affected hour is counted
  approximately and the share is clipped to 1.)
- Between two records a station's count is carried forward — the feed only
  writes changes, so no record means no change.
- ``bikes`` counts every bike type, so the mark is per station: the same
  value for every ``commodity_category`` of the facility. An hour with only
  electric bikes on the racks counts as having bikes.
- The feed's stations are matched to the trips' ``facility_id`` by
  coordinates: each facility (the median of its trip endpoint coordinates)
  takes the nearest feed station within ``MATCH_DISTANCE_M`` meters. A
  facility without a match keeps NaN — unknown, not zero.
- A matched station-hour with no zero-bike time gets share 0.0.
"""

from __future__ import annotations

import pathlib
from collections.abc import Callable

import numpy as np
import pandas as pd
import requests

from gbp.loaders.download import month_bounds, normalize_month, raw_dir

#: Monthly dump of the archived Citi Bike station feed.
STATUS_DUMP_URL = (
    "https://data.citybik.es/dumps/by-network/{year}/{month}-citi-bike-nyc-stats.parquet"
)

#: A facility and a feed station further apart than this are not the same place.
MATCH_DISTANCE_M = 100.0

#: The dump columns the mark needs; the rest of the dump is never read.
_STATUS_COLUMNS = ["nuid", "latitude", "longitude", "bikes", "timestamp"]

_HOUR = pd.Timedelta(hours=1)


def next_month(month: str) -> str:
    """Return the month after ``month``, as ``YYYYMM``."""
    _, end = month_bounds(month)
    return f"{end.year}{end.month:02d}"


def status_dump_path(month: str, raw: pathlib.Path | None = None) -> pathlib.Path:
    """Where one month's status dump lives, named as published by the archive."""
    return (raw or raw_dir()) / f"{normalize_month(month)}-citi-bike-nyc-stats.parquet"


def download_status_months(
    months: list[str],
    raw: pathlib.Path | None = None,
    log: Callable[[str], None] | None = None,
) -> list[pathlib.Path]:
    """Download the status dumps that are missing from ``data/raw/``.

    A month the archive has not published (before 2024-11, or not archived
    yet) is noted and skipped — the mark is best-effort, so a missing dump
    is not an error. Returns the newly downloaded files.
    """
    base = raw or raw_dir()
    base.mkdir(parents=True, exist_ok=True)
    say = log or (lambda message: None)
    downloaded: list[pathlib.Path] = []
    for month in [normalize_month(m) for m in months]:
        path = status_dump_path(month, base)
        if path.exists():
            say(f"{month}: status dump on disk, skipping")
            continue
        say(f"{month}: downloading the station-status dump ...")
        response = requests.get(STATUS_DUMP_URL.format(year=month[:4], month=month), timeout=300)
        if response.status_code == 404:
            say(f"{month}: the archive has no station-status dump")
            continue
        response.raise_for_status()
        path.write_bytes(response.content)
        downloaded.append(path)
    return downloaded


def load_status_records(month: str, raw: pathlib.Path | None = None) -> pd.DataFrame | None:
    """Read the records that cover one local month: its dump plus the next month's.

    The next month's dump holds the last local evening of ``month`` (the
    dumps cut at UTC month borders). Returns None when the month's own dump
    is not on disk — without it there is no usable coverage.
    """
    own = status_dump_path(month, raw)
    if not own.exists():
        return None
    frames = [pd.read_parquet(own, columns=_STATUS_COLUMNS)]
    tail = status_dump_path(next_month(month), raw)
    if tail.exists():
        frames.append(pd.read_parquet(tail, columns=_STATUS_COLUMNS))
    return pd.concat(frames, ignore_index=True)


def stockout_shares(status_records_df: pd.DataFrame, month: str) -> pd.DataFrame:
    """Compute per station the share of each local hour with zero bikes available.

    Records are status changes, so each record's value holds until the
    station's next record (the last one holds until the month ends). The
    zero-bike stretches are cut at hour borders and summed per hour.

    Parameters
    ----------
    status_records_df : pandas.DataFrame
        Feed records: ``nuid``, ``bikes``, ``timestamp`` (UTC, naive).
    month : str
        The local calendar month to cover, as ``YYYYMM`` or ``YYYY-MM``.

    Returns
    -------
    pandas.DataFrame
        ``nuid``, ``start_timestamp`` (local hour), ``stockout_share`` —
        only the hours with a positive share.
    """
    start, end = month_bounds(month)
    df = status_records_df[["nuid", "timestamp", "bikes"]].copy()
    df["timestamp"] = (
        df["timestamp"].dt.tz_localize("UTC").dt.tz_convert("America/New_York").dt.tz_localize(None)
    )
    df = df.sort_values(["nuid", "timestamp"], kind="stable").reset_index(drop=True)
    df["interval_end"] = df.groupby("nuid")["timestamp"].shift(-1).fillna(end)

    zero = df[df["bikes"] == 0].copy()
    zero["interval_start"] = zero["timestamp"].clip(lower=start)
    zero["interval_end"] = zero["interval_end"].clip(upper=end)
    zero = zero[zero["interval_start"] < zero["interval_end"]].reset_index(drop=True)
    if zero.empty:
        return pd.DataFrame(
            {
                "nuid": pd.Series(dtype=object),
                "start_timestamp": pd.Series(dtype="datetime64[ns]"),
                "stockout_share": pd.Series(dtype="float64"),
            }
        )

    # Cut each stretch at hour borders: repeat its row once per hour it
    # touches, give copy number i the i-th hour, then measure the overlap.
    first_hour = zero["interval_start"].dt.floor("h")
    last_hour = (zero["interval_end"] - pd.Timedelta(1, "ns")).dt.floor("h")
    hours_touched = ((last_hour - first_hour) // _HOUR + 1).astype("int64")
    repeated = zero.index.repeat(hours_touched)
    rep = zero.loc[repeated, ["nuid", "interval_start", "interval_end"]].reset_index(drop=True)
    block_starts = np.repeat(hours_touched.cumsum().shift(fill_value=0).to_numpy(), hours_touched)
    copy_number = np.arange(len(rep)) - block_starts
    rep["hour"] = first_hour.loc[repeated].reset_index(drop=True) + pd.to_timedelta(
        copy_number, unit="h"
    )
    overlap = np.minimum(rep["interval_end"], rep["hour"] + _HOUR) - np.maximum(
        rep["interval_start"], rep["hour"]
    )
    rep["stockout_share"] = overlap.dt.total_seconds() / 3600.0

    shares = rep.groupby(["nuid", "hour"], as_index=False)["stockout_share"].sum()
    shares["stockout_share"] = shares["stockout_share"].clip(0.0, 1.0)
    return shares.rename(columns={"hour": "start_timestamp"})


def facility_coords(trips_df: pd.DataFrame) -> pd.DataFrame:
    """Return each facility's location: the median of its trip endpoint coordinates."""
    starts = trips_df[["start_station_id", "start_lat", "start_lng"]]
    ends = trips_df[["end_station_id", "end_lat", "end_lng"]]
    named = ["facility_id", "latitude", "longitude"]
    points = pd.concat(
        [starts.set_axis(named, axis=1), ends.set_axis(named, axis=1)], ignore_index=True
    )
    return points.groupby("facility_id", as_index=False)[["latitude", "longitude"]].median()


def station_coords(status_records_df: pd.DataFrame) -> pd.DataFrame:
    """Return each feed station's location: the first coordinates in the records."""
    return status_records_df.groupby("nuid", as_index=False)[["latitude", "longitude"]].first()


def match_facilities(
    facility_coords_df: pd.DataFrame,
    station_coords_df: pd.DataFrame,
    max_distance_m: float = MATCH_DISTANCE_M,
) -> pd.DataFrame:
    """Match each facility to the nearest feed station within the cutoff.

    Distance is plain flat-map meters (good enough at city scale). A
    facility with no station inside ``max_distance_m`` is left out of the
    result.

    Returns
    -------
    pandas.DataFrame
        ``facility_id``, ``nuid`` — one row per matched facility.
    """
    if facility_coords_df.empty or station_coords_df.empty:
        return pd.DataFrame(
            {"facility_id": pd.Series(dtype=object), "nuid": pd.Series(dtype=object)}
        )
    meters_per_degree = 111_320.0
    lng_scale = float(np.cos(np.radians(station_coords_df["latitude"].mean())))
    fy = facility_coords_df["latitude"].to_numpy() * meters_per_degree
    fx = facility_coords_df["longitude"].to_numpy() * meters_per_degree * lng_scale
    sy = station_coords_df["latitude"].to_numpy() * meters_per_degree
    sx = station_coords_df["longitude"].to_numpy() * meters_per_degree * lng_scale

    squared = (fy[:, None] - sy[None, :]) ** 2 + (fx[:, None] - sx[None, :]) ** 2
    nearest = squared.argmin(axis=1)
    distance = np.sqrt(squared[np.arange(len(fy)), nearest])
    matched = distance <= max_distance_m
    return pd.DataFrame(
        {
            "facility_id": facility_coords_df["facility_id"].to_numpy()[matched],
            "nuid": station_coords_df["nuid"].to_numpy()[nearest[matched]],
        }
    )


def stockout_share_table(
    month: str,
    trips_df: pd.DataFrame,
    raw: pathlib.Path | None = None,
    status_records_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    """Build one month's mark keyed by facility, plus the list of covered facilities.

    Reads the dumps from disk (``download_status_months`` fetches them
    beforehand); pass ``status_records_df`` to skip the disk read. When the
    month's dump is absent the table is empty and no facility is covered —
    the ``stockout_share`` column of that month then stays NaN.

    Returns
    -------
    tuple of (pandas.DataFrame, list of str)
        Rows ``facility_id``, ``start_timestamp``, ``stockout_share`` for
        the hours with a positive share, and the facilities the feed covers
        (their remaining hours mean share 0.0, not unknown).
    """
    if status_records_df is None:
        status_records_df = load_status_records(month, raw)
    if status_records_df is None:
        empty = pd.DataFrame(
            {
                "facility_id": pd.Series(dtype=object),
                "start_timestamp": pd.Series(dtype="datetime64[ns]"),
                "stockout_share": pd.Series(dtype="float64"),
            }
        )
        return empty, []
    mapping = match_facilities(facility_coords(trips_df), station_coords(status_records_df))
    shares = stockout_shares(status_records_df, month)
    table = mapping.merge(shares, on="nuid", how="inner")
    return (
        table[["facility_id", "start_timestamp", "stockout_share"]],
        mapping["facility_id"].tolist(),
    )
