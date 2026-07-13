"""Forecasting-specific data helpers: the weather download and the month grid.

Downloading and loading the trip CSVs themselves is not here — that serves
both canonical runs and lives in ``gbp/loaders/download.py``. This module
keeps what only the forecasting pipeline needs:

- ``ml_dir`` — the root of the forecasting data, ``data/ml/``;
- ``month_period_grid`` — the hourly period grid of one calendar month
  (Notations.md §17), the one grid every month-shaped task counts on;
- ``load_weather_daily`` — the daily weather the feature builder joins in
  (plan, phase 3): maximum and minimum temperature and precipitation for the
  Central Park station of NOAA's GHCN-Daily dataset, served as CSV by the
  NCEI data service (no token needed). Weather is cached as one CSV per
  calendar year in ``data/raw/``; a past year's file never changes, and the
  current year's file is downloaded again when it does not yet reach the
  asked dates (NOAA publishes with a few days' delay).
"""

from __future__ import annotations

import os
import pathlib
from collections.abc import Callable

import pandas as pd
import requests

from gbp.loaders.dataloader_graph import DEFAULT_PERIOD_LEN, get_forecast_periods_df
from gbp.loaders.download import month_bounds, raw_dir

_DEFAULT_DATA_DIR = pathlib.Path(__file__).resolve().parents[2] / "data"


def ml_dir() -> pathlib.Path:
    """Root of the forecasting data (Notations.md §15): ``<data dir>/ml``.

    Honors the same ``DATA_DIR`` environment switch as ``app/artifacts.py``;
    without it, this is ``data/ml`` at the repository root.
    """
    return pathlib.Path(os.environ.get("DATA_DIR", _DEFAULT_DATA_DIR)) / "ml"


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
