"""Download monthly Citi Bike trip files and harmonize their two schemas.

This module is the data step of the forecasting plan (phase 2): given a list
of months, it fills ``data/raw/`` with the published trip CSVs, and it loads
any of those CSVs — old or new — into the one trips schema the rest of the
pipeline checks (``TRIPS_SCHEMA`` in ``gbp/loaders/dataloader_raw.py``).

It also fetches the daily weather the feature builder joins in (plan,
phase 3): maximum and minimum temperature and precipitation for the Central
Park station of NOAA's GHCN-Daily dataset, served as CSV by the NCEI data
service (no token needed). Weather is cached as one CSV per calendar year in
``data/raw/``; a past year's file never changes, and the current year's file
is downloaded again when it does not yet reach the asked dates (NOAA
publishes with a few days' delay).

The bucket
----------
Citi Bike publishes trips at ``https://s3.amazonaws.com/tripdata``. The key
names are not uniform, so this module lists the bucket by prefix and takes
what exists instead of building names from a fixed pattern:

- months from 2024 on: one zip per month, ``202601-citibike-tripdata.zip``;
- earlier years: one zip per year (``2019-citibike-tripdata.zip``) — the
  monthly zips were removed from the bucket. The downloader raises a clear
  error for such months; unpack the yearly bundle into ``data/raw/`` by hand.
- Jersey City is a separate small zip per month, usually
  ``JC-202512-citibike-tripdata.csv.zip`` but sometimes without the ``.csv``
  part. It is downloaded together with the matching city month.

A monthly zip holds one or more CSVs (large months are split into
``..._1.csv``, ``..._2.csv``); extraction keeps only the ``*.csv`` members,
flattened to their base names, and skips ``__MACOSX`` junk entries.

The two published schemas
-------------------------
The published columns changed in February 2021. Files from then on have::

    ride_id, rideable_type, started_at, ended_at,
    start_station_name, start_station_id, end_station_name, end_station_id,
    start_lat, start_lng, end_lat, end_lng, member_casual

Files before that have::

    tripduration, starttime, stoptime,
    start station id, start station name,
    start station latitude, start station longitude,
    end station id, end station name,
    end station latitude, end station longitude,
    bikeid, usertype, birth year, gender

(some months in 2016-2017 publish the same columns in title case, e.g.
``Start Time`` — header matching ignores case and spacing).

:func:`load_trips_any_schema` maps both to the single trips schema. New files
go through ``load_trips_raw_df`` unchanged. Old files are renamed and filled:

- ``starttime`` → ``started_at``, ``stoptime`` → ``ended_at``;
- ``start station id`` → ``start_station_id`` (kept as a string; the old ids
  are integers and the new ids are codes like ``"6140.05"`` — two different
  id spaces, so stations cannot be joined across the February 2021 boundary
  by id alone);
- ``start station latitude`` / ``longitude`` → ``start_lat`` / ``start_lng``
  (same for ``end``);
- ``usertype`` → ``member_casual`` (``Subscriber`` → ``member``,
  ``Customer`` → ``casual``);
- ``ride_id`` did not exist → null; ``rideable_type`` did not exist → every
  old trip is a ``classic_bike``;
- ``tripduration``, ``bikeid``, ``birth year``, ``gender`` are dropped.

Files of both eras carry a few unusable rows — a missing key field, an
endpoint far outside the service area (a test dock, another city), or a trip
that seems to end before it starts on the fall-back night of daylight saving
time. Both loading paths drop them through the one shared cleaning step
(``clean_trips`` in ``dataloader_raw.py``).

Terminal use::

    python -m gbp.ml.data --months 202502 202503
"""

from __future__ import annotations

import argparse
import os
import pathlib
import re
import shutil
import xml.etree.ElementTree as ET
import zipfile
from collections.abc import Callable

import pandas as pd
import requests

from gbp.loaders.dataloader_graph import DEFAULT_PERIOD_LEN, get_forecast_periods_df
from gbp.loaders.dataloader_raw import (
    TRIPS_SCHEMA,
    clean_trips,
    load_trips_raw_df,
    processed_trips_path,
)
from gbp.model.journal_schema import schema_violations

TRIPDATA_BUCKET_URL = "https://s3.amazonaws.com/tripdata"

_DEFAULT_DATA_DIR = pathlib.Path(__file__).resolve().parents[2] / "data"

#: Extra strings the old CSVs use for a missing value.
_NA_VALUES = ["NULL", "\\N"]


def raw_dir() -> pathlib.Path:
    """Return the download target (Notations.md §15): ``<data dir>/raw``.

    Honors the same ``DATA_DIR`` environment switch as the rest of the
    project; without it, this is ``data/raw`` at the repository root.
    """
    return pathlib.Path(os.environ.get("DATA_DIR", _DEFAULT_DATA_DIR)) / "raw"


def ml_dir() -> pathlib.Path:
    """Root of the forecasting data (Notations.md §15): ``<data dir>/ml``.

    Honors the same ``DATA_DIR`` environment switch as ``app/artifacts.py``;
    without it, this is ``data/ml`` at the repository root.
    """
    return pathlib.Path(os.environ.get("DATA_DIR", _DEFAULT_DATA_DIR)) / "ml"


def normalize_month(month: str) -> str:
    """Turn a month given as ``YYYYMM`` or ``YYYY-MM`` into ``YYYYMM``.

    Raises ``ValueError`` for anything that is not a real month.
    """
    compact = month.replace("-", "")
    if not re.fullmatch(r"\d{6}", compact) or not 1 <= int(compact[4:]) <= 12:
        raise ValueError(f"not a month: {month!r} (expected YYYYMM or YYYY-MM)")
    return compact


def month_bounds(month: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return the wall-clock start of a month and the start of the next one."""
    month = normalize_month(month)
    start = pd.Timestamp(year=int(month[:4]), month=int(month[4:]), day=1)
    return start, start + pd.offsets.MonthBegin(1)


def month_period_grid(month: str) -> pd.DataFrame:
    """Build the hourly period grid of one calendar month (Notations.md §17).

    One row per hour of the month: ``period_id`` — 0 at the month's first
    hour — plus the hour's ``start_timestamp`` and ``end_timestamp``. Every
    month-shaped task counts on this one grid: a training partition numbers
    its hours on it, and a forecast of a held-out month uses it as the
    forecast horizon (the backtest and the monitoring baseline).
    """
    start, end = month_bounds(month)
    return get_forecast_periods_df(
        start, int((end - start) / DEFAULT_PERIOD_LEN), DEFAULT_PERIOD_LEN
    )


def month_csvs(month: str, raw: pathlib.Path | None = None) -> list[pathlib.Path]:
    """Return the raw CSVs of one month already on disk, city and Jersey City alike.

    Matches by the published naming (``...{YYYYMM}-citibike-tripdata...``), so
    ``202601-citibike-tripdata_1.csv`` and ``JC-202601-citibike-tripdata.csv``
    both count as month ``202601``.
    """
    month = normalize_month(month)
    base = raw or raw_dir()
    if not base.exists():
        return []
    return sorted(p for p in base.glob("*.csv") if f"{month}-citibike-tripdata" in p.name)


def raw_trip_months(raw: pathlib.Path | None = None) -> list[str]:
    """Return the months whose trip CSVs are on disk, sorted, as ``YYYYMM``.

    Reads the months off the published file naming, the same match
    :func:`month_csvs` uses per month.
    """
    base = raw or raw_dir()
    if not base.exists():
        return []
    found = re.findall(r"(\d{6})-citibike-tripdata", " ".join(p.name for p in base.glob("*.csv")))
    return sorted(set(found))


def _list_bucket_keys(prefix: str) -> list[str]:
    """List the bucket keys under one prefix (the S3 ``list-type=2`` API)."""
    response = requests.get(
        f"{TRIPDATA_BUCKET_URL}/", params={"list-type": "2", "prefix": prefix}, timeout=30
    )
    response.raise_for_status()
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    root = ET.fromstring(response.content)
    return [el.text for el in root.findall("s3:Contents/s3:Key", ns) if el.text]


def month_zip_keys(month: str) -> list[str]:
    """Return the bucket keys to download for one month: the city zip plus Jersey City.

    Raises ``ValueError`` when the bucket has no city zip for the month — that
    is the case for months before 2024, which now exist only inside yearly
    bundles (``<YYYY>-citibike-tripdata.zip``); those are unpacked by hand.
    """
    month = normalize_month(month)
    keys = [
        key
        for prefix in (f"{month}-citibike-tripdata", f"JC-{month}-citibike-tripdata")
        for key in _list_bucket_keys(prefix)
        if key.endswith(".zip")
    ]
    if not any(key.startswith(month) for key in keys):
        raise ValueError(
            f"the bucket has no monthly zip for {month}; months before 2024 exist only "
            f"as the yearly bundle {month[:4]}-citibike-tripdata.zip — download and "
            f"unpack it into data/raw/ by hand"
        )
    return keys


def _download(url: str, dest: pathlib.Path) -> None:
    """Stream one file from ``url`` to ``dest`` (the zips are hundreds of MB)."""
    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with open(dest, "wb") as out:
            for chunk in response.iter_content(chunk_size=1 << 20):
                out.write(chunk)


def _extract_csvs(zip_path: pathlib.Path, dest: pathlib.Path) -> list[pathlib.Path]:
    """Unpack the ``*.csv`` members of one zip into ``dest``, flat.

    Members are written under their base name (any folder inside the zip is
    dropped). Non-CSV members, ``__MACOSX`` entries and hidden files are
    skipped.
    """
    extracted: list[pathlib.Path] = []
    with zipfile.ZipFile(zip_path) as archive:
        for member in archive.infolist():
            name = pathlib.PurePosixPath(member.filename).name
            junk = (
                member.is_dir()
                or "__MACOSX" in member.filename
                or name.startswith(".")
                or not name.endswith(".csv")
            )
            if junk:
                continue
            target = dest / name
            with archive.open(member) as src, open(target, "wb") as out:
                shutil.copyfileobj(src, out)
            extracted.append(target)
    return extracted


def download_months(
    months: list[str],
    raw: pathlib.Path | None = None,
    log: Callable[[str], None] | None = None,
) -> list[pathlib.Path]:
    """Download the monthly zips and leave their CSVs in ``data/raw/``.

    A month whose CSVs are already on disk (:func:`month_csvs`) is skipped
    whole — ``data/raw/`` is never edited, so a re-download means deleting the
    month's CSVs first. Each zip is downloaded next to its CSVs, unpacked with
    :func:`_extract_csvs`, and deleted. Returns the newly extracted files.
    """
    base = raw or raw_dir()
    base.mkdir(parents=True, exist_ok=True)
    say = log or (lambda message: None)
    extracted: list[pathlib.Path] = []
    for month in [normalize_month(m) for m in months]:
        present = month_csvs(month, base)
        if present:
            say(f"{month}: already on disk ({len(present)} files), skipping")
            continue
        for key in month_zip_keys(month):
            say(f"{month}: downloading {key} ...")
            zip_path = base / key.replace("/", "_")
            try:
                _download(f"{TRIPDATA_BUCKET_URL}/{key}", zip_path)
                files = _extract_csvs(zip_path, base)
            finally:
                zip_path.unlink(missing_ok=True)
            say(f"{month}: extracted {', '.join(f.name for f in files)}")
            extracted.extend(files)
    return extracted


# ---------------------------------------------------------------------------
# Daily weather (NOAA GHCN-Daily, Central Park)
# ---------------------------------------------------------------------------
#: NOAA GHCN-Daily id of the Central Park station.
WEATHER_STATION_ID = "USW00094728"

#: The NCEI data service that serves GHCN-Daily as CSV without a token.
_NCEI_DATA_URL = "https://www.ncei.noaa.gov/access/services/data/v1"

#: Published data type -> the weather column it becomes (units: metric).
_WEATHER_COLUMNS = {
    "TMAX": "temperature_max_c",
    "TMIN": "temperature_min_c",
    "PRCP": "precipitation_mm",
}


def weather_year_path(year: int, raw: pathlib.Path | None = None) -> pathlib.Path:
    """Where one year of daily weather lives: ``<raw dir>/weather-central-park-<year>.csv``."""
    return (raw or raw_dir()) / f"weather-central-park-{year}.csv"


def _download_weather_year(year: int, dest: pathlib.Path) -> None:
    """Fetch one calendar year of daily TMAX/TMIN/PRCP for the Central Park station."""
    response = requests.get(
        _NCEI_DATA_URL,
        params={
            "dataset": "daily-summaries",
            "stations": WEATHER_STATION_ID,
            "dataTypes": ",".join(_WEATHER_COLUMNS),
            "startDate": f"{year}-01-01",
            "endDate": f"{year}-12-31",
            "format": "csv",
            "units": "metric",
        },
        timeout=120,
    )
    response.raise_for_status()
    dest.write_bytes(response.content)


def _read_weather_csv(path: pathlib.Path) -> pd.DataFrame:
    """Read one cached year file into the weather columns (``date`` plus the three values)."""
    try:
        raw_df = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        raw_df = pd.DataFrame(columns=["DATE", *_WEATHER_COLUMNS])
    out = pd.DataFrame({"date": pd.to_datetime(raw_df.get("DATE", pd.Series(dtype="string")))})
    for source, column in _WEATHER_COLUMNS.items():
        if source in raw_df.columns:
            out[column] = pd.to_numeric(raw_df[source], errors="coerce").astype("float64")
        else:
            out[column] = pd.Series(pd.NA, index=out.index, dtype="float64")
    out = out.dropna(subset=["date"]).drop_duplicates(subset="date", keep="first")
    return out.sort_values("date").reset_index(drop=True)


def load_weather_daily(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    raw: pathlib.Path | None = None,
    log: Callable[[str], None] | None = None,
) -> pd.DataFrame:
    """Return the daily Central Park weather over ``[start_date, end_date]``, both ends included.

    One row per date: ``date``, ``temperature_max_c``, ``temperature_min_c``,
    ``precipitation_mm``. A date the station has not reported yet is simply
    absent — the weather join (``add_weather_features``) then leaves NaN.

    Year files already on disk are used as they are; a year file that does
    not reach the asked dates yet is downloaded again. Asking for dates NOAA
    has not published (for example, today) therefore re-downloads the current
    year's file on every call — pass only dates that can exist.
    """
    start = pd.Timestamp(start_date).normalize()
    end = pd.Timestamp(end_date).normalize()
    if start > end:
        raise ValueError(f"weather range is empty: {start.date()} > {end.date()}")
    base = raw or raw_dir()
    base.mkdir(parents=True, exist_ok=True)
    say = log or (lambda message: None)

    frames = []
    for year in range(start.year, end.year + 1):
        path = weather_year_path(year, base)
        need_until = min(end, pd.Timestamp(year=year, month=12, day=31))
        year_df = _read_weather_csv(path) if path.exists() else None
        if year_df is None or year_df.empty or year_df["date"].max() < need_until:
            say(f"weather {year}: downloading {WEATHER_STATION_ID} daily summaries ...")
            _download_weather_year(year, path)
            year_df = _read_weather_csv(path)
        if year_df.empty or year_df["date"].max() < need_until:
            say(f"weather {year}: published data ends before {need_until.date()}")
        frames.append(year_df)
    out = pd.concat(frames, ignore_index=True)
    return out[(out["date"] >= start) & (out["date"] <= end)].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Schema harmonization
# ---------------------------------------------------------------------------
def _squash(column: str) -> str:
    """Reduce a header name to lowercase letters: ``"Start Time"`` → ``"starttime"``."""
    return re.sub(r"[^a-z]", "", column.lower())


#: Old header (squashed) → the column it becomes in the new schema.
_OLD_TO_NEW = {
    "starttime": "started_at",
    "stoptime": "ended_at",
    "startstationid": "start_station_id",
    "startstationname": "start_station_name",
    "startstationlatitude": "start_lat",
    "startstationlongitude": "start_lng",
    "endstationid": "end_station_id",
    "endstationname": "end_station_name",
    "endstationlatitude": "end_lat",
    "endstationlongitude": "end_lng",
    "usertype": "usertype",
}


def _read_old_schema_csv(trips_path: str) -> pd.DataFrame:
    """Read one pre-2021 CSV and return it in the new-schema columns.

    Implements the mapping in the module docstring: rename the columns, keep
    station ids as strings, fill the columns the old files did not have
    (``ride_id`` null, ``rideable_type`` = ``classic_bike``), map ``usertype``
    to ``member_casual``, and drop the unusable rows (``clean_trips``).
    """
    header = pd.read_csv(trips_path, nrows=0)
    rename = {c: _OLD_TO_NEW[_squash(c)] for c in header.columns if _squash(c) in _OLD_TO_NEW}
    string_columns = {"start_station_id", "end_station_id"}
    dtypes = {c: "string" for c, new in rename.items() if new in string_columns}
    df = pd.read_csv(trips_path, dtype=dtypes, na_values=_NA_VALUES).rename(columns=rename)

    trips_df = pd.DataFrame(
        {
            "ride_id": pd.Series(pd.NA, index=df.index, dtype="string"),
            "rideable_type": pd.Series("classic_bike", index=df.index, dtype="string"),
            "started_at": pd.to_datetime(df["started_at"], format="mixed"),
            "ended_at": pd.to_datetime(df["ended_at"], format="mixed"),
            "start_station_name": df["start_station_name"].astype("string"),
            "start_station_id": df["start_station_id"],
            "end_station_name": df["end_station_name"].astype("string"),
            "end_station_id": df["end_station_id"],
            "start_lat": df["start_lat"].astype("float64"),
            "start_lng": df["start_lng"].astype("float64"),
            "end_lat": df["end_lat"].astype("float64"),
            "end_lng": df["end_lng"].astype("float64"),
            "member_casual": df["usertype"]
            .map({"Subscriber": "member", "Customer": "casual"})
            .astype("string"),
        }
    )
    return clean_trips(trips_df)


def load_trips_any_schema(trips_path: str) -> pd.DataFrame:
    """Load one trip CSV of either era into the single trips schema.

    New-schema files (February 2021 on, recognized by their ``started_at``
    column) go through ``load_trips_raw_df`` unchanged. Old-schema files are
    mapped with :func:`_read_old_schema_csv`, checked against the same
    ``TRIPS_SCHEMA``, and cached in ``data/processed/`` the same way — the
    first load parses the CSV, later loads read the parquet copy.
    """
    header = pd.read_csv(trips_path, nrows=0)
    if "started_at" in header.columns:
        return load_trips_raw_df(trips_path)

    csv = pathlib.Path(trips_path)
    processed = processed_trips_path(trips_path)
    processed_is_fresh = processed.exists() and processed.stat().st_mtime >= csv.stat().st_mtime
    if processed_is_fresh:
        trips_df = pd.read_parquet(processed)
    else:
        trips_df = _read_old_schema_csv(trips_path)
    violations = schema_violations(TRIPS_SCHEMA, trips_df)
    if violations:
        read_from = processed if processed_is_fresh else csv
        raise ValueError(
            f"trips table {read_from} breaks the trips schema:\n" + "\n".join(violations)
        )
    if not processed_is_fresh:
        processed.parent.mkdir(parents=True, exist_ok=True)
        trips_df.to_parquet(processed, index=False)
    return trips_df


def main() -> None:
    """Terminal entry point: download the given months into ``data/raw/``."""
    parser = argparse.ArgumentParser(
        description="Download monthly Citi Bike trip files into data/raw/."
    )
    parser.add_argument(
        "--months",
        nargs="+",
        required=True,
        help="months to download, as YYYYMM or YYYY-MM",
    )
    args = parser.parse_args()
    files = download_months(args.months, log=print)
    print(f"Done: {len(files)} new files in {raw_dir()}")


if __name__ == "__main__":
    main()
