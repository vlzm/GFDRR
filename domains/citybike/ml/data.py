"""Forecasting-specific data helpers: the weather download and the month grid."""

from __future__ import annotations

import dataclasses
import pathlib
from collections.abc import Callable

import pandas as pd
import requests

from domains.citybike.loaders.download import month_bounds, raw_dir
from gbp.ml.artifact import forecasts_root
from gbp.model.dataloader_graph import DEFAULT_PERIOD_LEN, PeriodGrid


@dataclasses.dataclass(frozen=True)
class MlPaths:
    """The data folders the forecasting pipeline reads and writes."""

    raw: pathlib.Path
    training: pathlib.Path
    forecasts: pathlib.Path
    monitoring: pathlib.Path
    tracking: pathlib.Path | None = None

    @classmethod
    def resolve(cls) -> MlPaths:
        """Return the repository default folders (honoring the ``DATA_DIR`` switch)."""
        # Imported here, not at the top: training and monitoring each import
        # this module, so importing them at the top would cycle.
        from domains.citybike.ml.ops.monitoring import monitoring_dir
        from domains.citybike.ml.training import training_dir

        return cls(
            raw=raw_dir(),
            training=training_dir(),
            forecasts=forecasts_root(),
            monitoring=monitoring_dir(),
        )

    @classmethod
    def under(cls, base: pathlib.Path, *, tracking: pathlib.Path | None = None) -> MlPaths:
        """Point every folder at a subfolder of ``base`` — the layout a test builds."""
        return cls(
            raw=base / "raw",
            training=base / "training",
            forecasts=base / "forecasts",
            monitoring=base / "monitoring",
            tracking=tracking,
        )


def month_grid(month: str) -> PeriodGrid:
    """Build the hourly period grid of one calendar month, as a PeriodGrid."""
    start, end = month_bounds(month)
    return PeriodGrid(start, int((end - start) / DEFAULT_PERIOD_LEN), DEFAULT_PERIOD_LEN)


def month_period_grid(month: str) -> pd.DataFrame:
    """Build the hourly period grid of one calendar month."""
    return month_grid(month).frame()


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
    """Return the daily Central Park weather over ``[start_date, end_date]``, both ends included."""
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
