"""Build the training table (Notations.md §17) from the raw trip CSVs.

The training table is the table a model learns from: one row per
``(period_id, facility_id, commodity_category)`` with the observed departure
count in ``quantity``, the feature columns (``gbp/ml/features.py``), and the
censoring mark ``stockout_share`` (``gbp/ml/station_status.py``). Zero count
rows are kept — a station-hour with no departures is a real observation, not
a missing one. Periods are hours (``DEFAULT_PERIOD_LEN``), numbered from 0
at the start of the month, and each row also carries the hour's wall-clock
``start_timestamp`` so months can be lined up.

One month becomes one parquet partition, ``data/ml/training/<YYYYMM>.parquet``.
A partition covers every hour of its calendar month and every
``(station, bike type)`` seen in that month's trips — a station that only
received bikes still gets its zero rows. Trips are counted into the month
they started in; a published file sometimes carries a few trips of the
neighbor month (the January 2026 file starts on December 31), and the month
filter keeps those out of the wrong partition.

A month's features are built as if that month were being forecast: they read
only the departure counts of the ``HISTORY_WEEKS`` weeks before the month,
taken from the earlier partitions on disk. Build months oldest-first (the
terminal command sorts them) so every month finds its history; the oldest
month has none, and its history features stay NaN. This is what makes the
training table and the forecast input agree — the test
``test_training_table_and_forecast_input_agree_for_the_same_station_day``
holds the two together.

The observed departure count here equals what the simulator's own read-model
gives: counting trips by start hour is the same as building the historical
flow journal and counting its ``departed`` events (``flows_to_departures``) —
a test in ``tests/test_ml_training.py`` holds the two together.

Terminal use (one command: download what is missing, then build)::

    python -m gbp.ml.training --months 202502 202503 202504
"""

from __future__ import annotations

import argparse
import pathlib

import pandas as pd
import pandera.pandas as pa

from gbp.loaders.dataloader_graph import DEFAULT_PERIOD_LEN, to_period_id
from gbp.loaders.download import (
    download_months,
    load_trips_any_schema,
    month_bounds,
    month_csvs,
    normalize_month,
)
from gbp.ml.data import load_weather_daily, ml_dir, month_period_grid
from gbp.ml.features import FEATURE_SCHEMA_COLUMNS, HISTORY_WEEKS, build_features
from gbp.ml.station_status import download_status_months, next_month, stockout_share_table
from gbp.model.journal_schema import schema_violations

#: One row per station-hour and bike type, zero rows kept: the departure
#: count, the feature columns, and the censoring mark ``stockout_share``
#: (NaN where no station-status snapshot covers the hour).
TRAINING_TABLE_SCHEMA = pa.DataFrameSchema(
    columns={
        "period_id": pa.Column("int64", pa.Check.ge(0), nullable=False),
        "start_timestamp": pa.Column("datetime64[ns]", nullable=False),
        "facility_id": pa.Column(nullable=False),
        "commodity_category": pa.Column(nullable=False),
        "quantity": pa.Column("int64", pa.Check.ge(0), nullable=False),
        **FEATURE_SCHEMA_COLUMNS,
        "stockout_share": pa.Column("float64", pa.Check.in_range(0, 1), nullable=True),
    },
    unique=["period_id", "facility_id", "commodity_category"],
    strict=False,
    name="training_table",
)


def training_dir() -> pathlib.Path:
    """Folder of the monthly training partitions: ``<ml dir>/training``."""
    return ml_dir() / "training"


def partition_path(month: str, root: pathlib.Path | None = None) -> pathlib.Path:
    """Where one month's partition lives: ``<training dir>/<YYYYMM>.parquet``."""
    return (root or training_dir()) / f"{normalize_month(month)}.parquet"


def load_actual_month(month: str, root: pathlib.Path | None = None) -> pd.DataFrame:
    """Read one month's actual departure counts off its training partition.

    Only the count columns are read — the callers (the backtest, monitoring,
    the evaluation) score forecasts against the counts and do not need the
    feature columns. Raises ``FileNotFoundError`` when the partition is
    missing.
    """
    path = partition_path(month, root)
    if not path.exists():
        raise FileNotFoundError(
            f"no training partition for {normalize_month(month)}; "
            "build it first (python -m gbp.ml.training)"
        )
    return pd.read_parquet(
        path, columns=["period_id", "facility_id", "commodity_category", "quantity"]
    )


def history_months(month: str) -> list[str]:
    """Return the months whose partitions overlap the history window of ``month``."""
    start, _ = month_bounds(month)
    window_start = start - HISTORY_WEEKS * pd.Timedelta(weeks=1)
    last_hour = start - pd.Timedelta(hours=1)
    return [str(p) for p in pd.period_range(window_start, last_hour, freq="M").strftime("%Y%m")]


def load_history_counts(month: str, root: pathlib.Path | None = None) -> pd.DataFrame:
    """Read the departure counts available before ``month``: the earlier partitions on disk.

    Only the partitions inside the history window are read, and only their
    count columns — the feature columns of the earlier months are not
    needed. A window month whose partition is missing is skipped, so build
    months oldest-first; the oldest month simply has no history.
    """
    columns = ["start_timestamp", "facility_id", "commodity_category", "quantity"]
    paths = [partition_path(m, root) for m in history_months(month)]
    frames = [pd.read_parquet(path, columns=columns) for path in paths if path.exists()]
    if not frames:
        return pd.DataFrame(
            {
                "start_timestamp": pd.Series(dtype="datetime64[ns]"),
                "facility_id": pd.Series(dtype=object),
                "commodity_category": pd.Series(dtype=object),
                "quantity": pd.Series(dtype="int64"),
            }
        )
    return pd.concat(frames, ignore_index=True)


def load_training_table(months: list[str], root: pathlib.Path | None = None) -> pd.DataFrame:
    """Read the training table of the given months: their partitions stacked.

    A year of partitions is tens of millions of rows, so the columns are
    made compact while loading: the float feature columns become ``float32``,
    and ``facility_id`` / ``commodity_category`` become pandas ``category``
    columns (each distinct string stored once). All partitions share one
    category list, so the stacked table keeps the category dtype. Code that
    groups by these columns must pass ``observed=True``.

    Raises ``FileNotFoundError`` when a month has no partition — build it
    first with ``python -m gbp.ml.training``.
    """
    float_columns = [
        column
        for column, checked in TRAINING_TABLE_SCHEMA.columns.items()
        if str(checked.dtype) == "float64"
    ]
    id_columns = ["facility_id", "commodity_category"]
    frames = []
    for month in sorted(normalize_month(m) for m in months):
        path = partition_path(month, root)
        if not path.exists():
            raise FileNotFoundError(
                f"no training partition for {month}; build it first (python -m gbp.ml.training)"
            )
        frame = pd.read_parquet(path)
        frame[float_columns] = frame[float_columns].astype("float32")
        frames.append(frame)
    categories = {
        column: sorted({value for frame in frames for value in frame[column].unique()})
        for column in id_columns
    }
    for frame in frames:
        for column, values in categories.items():
            frame[column] = pd.Categorical(frame[column], categories=values)
    return pd.concat(frames, ignore_index=True)


def departure_counts(trips_df: pd.DataFrame, month: str) -> pd.DataFrame:
    """Count departures per station-hour and bike type over one full month.

    Keeps the trips that started inside the month, numbers the month's hours
    on the month period grid (``period_id`` 0 at the month's first hour,
    :func:`month_period_grid`), and counts each trip into the hour and
    station it started at. The result is the full
    grid: every hour of the month × every station seen in the month's trips
    (as a start or an end point) × every bike type seen — hours with no
    departures hold ``quantity`` 0.

    Parameters
    ----------
    trips_df : pandas.DataFrame
        Trips in the single trips schema (``load_trips_any_schema``).
    month : str
        The calendar month to count, as ``YYYYMM`` or ``YYYY-MM``.

    Returns
    -------
    pandas.DataFrame
        The month's slice of the training table (``TRAINING_TABLE_SCHEMA``),
        sorted by ``period_id``, ``facility_id``, ``commodity_category``.
    """
    start, end = month_bounds(month)
    in_month = trips_df[(trips_df["started_at"] >= start) & (trips_df["started_at"] < end)]
    if in_month.empty:
        raise ValueError(f"no trips start inside {normalize_month(month)}")

    month_periods_df = month_period_grid(month)
    facilities = pd.concat([in_month["start_station_id"], in_month["end_station_id"]]).unique()
    commodities = in_month["rideable_type"].unique()

    counts = (
        in_month.assign(period_id=to_period_id(in_month["started_at"], start, DEFAULT_PERIOD_LEN))
        .groupby(["period_id", "start_station_id", "rideable_type"])
        .size()
    )
    grid = pd.MultiIndex.from_product(
        [month_periods_df["period_id"], sorted(facilities), sorted(commodities)],
        names=["period_id", "facility_id", "commodity_category"],
    )
    table = (
        counts.rename_axis(grid.names).reindex(grid, fill_value=0).rename("quantity").reset_index()
    )
    starts = month_periods_df.set_index("period_id")["start_timestamp"]
    table.insert(1, "start_timestamp", table["period_id"].map(starts))
    return table


def build_month_partition(
    month: str,
    raw: pathlib.Path | None = None,
    root: pathlib.Path | None = None,
    weather_df: pd.DataFrame | None = None,
    status_records_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build one month's slice of the training table from the raw CSVs on disk.

    Loads every raw CSV of the month (city and Jersey City files alike),
    counts departures with :func:`departure_counts`, appends the feature
    columns (:func:`build_features`, history read from the earlier
    partitions under ``root``) and the ``stockout_share`` mark, and checks
    the result against ``TRAINING_TABLE_SCHEMA``.

    Without ``weather_df`` the month's weather is loaded through
    ``load_weather_daily``, which downloads the year file when it is not
    cached yet. Without ``status_records_df`` the status dumps are read from
    disk only (``download_status_months`` fetches them beforehand); when the
    month has no dump, ``stockout_share`` stays NaN.
    """
    month = normalize_month(month)
    csvs = month_csvs(month, raw)
    if not csvs:
        raise FileNotFoundError(
            f"no raw CSVs for {month}; download them first (python -m gbp.loaders.download)"
        )
    trips_df = pd.concat([load_trips_any_schema(str(path)) for path in csvs], ignore_index=True)
    table = departure_counts(trips_df, month)

    if weather_df is None:
        start, end = month_bounds(month)
        weather_df = load_weather_daily(start, end - pd.Timedelta(days=1), raw)
    table = build_features(table, load_history_counts(month, root), weather_df)

    shares, covered = stockout_share_table(month, trips_df, raw, status_records_df)
    table = table.merge(shares, on=["facility_id", "start_timestamp"], how="left")
    covered_rows = table["facility_id"].isin(set(covered))
    table.loc[covered_rows, "stockout_share"] = table.loc[covered_rows, "stockout_share"].fillna(
        0.0
    )
    table["stockout_share"] = table["stockout_share"].astype("float64")

    violations = schema_violations(TRAINING_TABLE_SCHEMA, table)
    if violations:
        raise ValueError(
            f"training partition {month} breaks the training-table schema:\n"
            + "\n".join(violations)
        )
    return table


def write_month_partition(
    month: str,
    raw: pathlib.Path | None = None,
    root: pathlib.Path | None = None,
    weather_df: pd.DataFrame | None = None,
    status_records_df: pd.DataFrame | None = None,
) -> pathlib.Path:
    """Build one month's partition and write it to ``data/ml/training/``."""
    table = build_month_partition(month, raw, root, weather_df, status_records_df)
    path = partition_path(month, root)
    path.parent.mkdir(parents=True, exist_ok=True)
    table.to_parquet(path, index=False)
    return path


def main() -> None:
    """Terminal entry point: months in, partitioned training parquet out.

    Idempotent end to end: a month already in ``data/raw/`` is not downloaded
    again, a partition already in ``data/ml/training/`` is not rebuilt
    (``--force`` rebuilds), so rerunning the command changes nothing.
    """
    parser = argparse.ArgumentParser(
        description="Download the given months and build their training-table partitions."
    )
    parser.add_argument(
        "--months", nargs="+", required=True, help="months to build, as YYYYMM or YYYY-MM"
    )
    parser.add_argument(
        "--force", action="store_true", help="rebuild partitions that already exist"
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="use only the raw files already on disk (weather is still fetched when missing)",
    )
    args = parser.parse_args()

    # Oldest first: a month's features read the earlier partitions.
    months = sorted(normalize_month(m) for m in args.months)
    if not args.no_download:
        download_months(months, log=print)
        status_months = sorted({m for month in months for m in (month, next_month(month))})
        download_status_months(status_months, log=print)
    for month in months:
        path = partition_path(month)
        if path.exists() and not args.force:
            print(f"{month}: partition exists, skipping")
            continue
        write_month_partition(month)
        built = pd.read_parquet(path)
        print(
            f"{month}: {len(built):,} rows "
            f"({built['facility_id'].nunique()} facilities, "
            f"{built['quantity'].sum():,} departures, "
            f"stockout mark on {built['stockout_share'].notna().mean():.0%} of rows) -> {path}"
        )


if __name__ == "__main__":
    main()
