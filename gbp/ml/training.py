"""Build the training table (Notations.md §17) from the raw trip CSVs.

The training table is the table a model learns from: one row per
``(period_id, facility_id, commodity_category)`` with the observed departure
count in ``quantity``. Zero rows are kept — a station-hour with no departures
is a real observation, not a missing one. Periods are hours
(``DEFAULT_PERIOD_LEN``), numbered from 0 at the start of the month, and each
row also carries the hour's wall-clock ``start_timestamp`` so months can be
lined up and calendar features derived later (plan, phase 3).

One month becomes one parquet partition, ``data/ml/training/<YYYYMM>.parquet``.
A partition covers every hour of its calendar month and every
``(station, bike type)`` seen in that month's trips — a station that only
received bikes still gets its zero rows. Trips are counted into the month
they started in; a published file sometimes carries a few trips of the
neighbor month (the January 2026 file starts on December 31), and the month
filter keeps those out of the wrong partition.

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
from gbp.ml.data import download_months, load_trips_any_schema, month_csvs, normalize_month
from gbp.ml.forecast import ml_dir
from gbp.model.journal_schema import schema_violations

#: One row per station-hour and bike type, zero rows kept.
TRAINING_TABLE_SCHEMA = pa.DataFrameSchema(
    columns={
        "period_id": pa.Column("int64", pa.Check.ge(0), nullable=False),
        "start_timestamp": pa.Column("datetime64[ns]", nullable=False),
        "facility_id": pa.Column(nullable=False),
        "commodity_category": pa.Column(nullable=False),
        "quantity": pa.Column("int64", pa.Check.ge(0), nullable=False),
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


def month_bounds(month: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return the wall-clock start of a month and the start of the next one."""
    month = normalize_month(month)
    start = pd.Timestamp(year=int(month[:4]), month=int(month[4:]), day=1)
    return start, start + pd.offsets.MonthBegin(1)


def departure_counts(trips_df: pd.DataFrame, month: str) -> pd.DataFrame:
    """Count departures per station-hour and bike type over one full month.

    Keeps the trips that started inside the month, numbers the month's hours
    as ``period_id`` 0, 1, 2, … from the month's first hour, and counts each
    trip into the hour and station it started at. The result is the full
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

    n_periods = int((end - start) / DEFAULT_PERIOD_LEN)
    facilities = pd.concat([in_month["start_station_id"], in_month["end_station_id"]]).unique()
    commodities = in_month["rideable_type"].unique()

    counts = (
        in_month.assign(period_id=to_period_id(in_month["started_at"], start, DEFAULT_PERIOD_LEN))
        .groupby(["period_id", "start_station_id", "rideable_type"])
        .size()
    )
    grid = pd.MultiIndex.from_product(
        [range(n_periods), sorted(facilities), sorted(commodities)],
        names=["period_id", "facility_id", "commodity_category"],
    )
    table = (
        counts.rename_axis(grid.names).reindex(grid, fill_value=0).rename("quantity").reset_index()
    )
    table.insert(1, "start_timestamp", start + table["period_id"] * DEFAULT_PERIOD_LEN)
    return table


def build_month_partition(month: str, raw: pathlib.Path | None = None) -> pd.DataFrame:
    """Build one month's slice of the training table from the raw CSVs on disk.

    Loads every raw CSV of the month (city and Jersey City files alike),
    counts departures with :func:`departure_counts`, and checks the result
    against ``TRAINING_TABLE_SCHEMA``.
    """
    month = normalize_month(month)
    csvs = month_csvs(month, raw)
    if not csvs:
        raise FileNotFoundError(
            f"no raw CSVs for {month}; download them first (python -m gbp.ml.data)"
        )
    trips_df = pd.concat([load_trips_any_schema(str(path)) for path in csvs], ignore_index=True)
    table = departure_counts(trips_df, month)
    violations = schema_violations(TRAINING_TABLE_SCHEMA, table)
    if violations:
        raise ValueError(
            f"training partition {month} breaks the training-table schema:\n"
            + "\n".join(violations)
        )
    return table


def write_month_partition(
    month: str, raw: pathlib.Path | None = None, root: pathlib.Path | None = None
) -> pathlib.Path:
    """Build one month's partition and write it to ``data/ml/training/``."""
    table = build_month_partition(month, raw)
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
        "--no-download", action="store_true", help="use only the raw files already on disk"
    )
    args = parser.parse_args()

    months = [normalize_month(m) for m in args.months]
    if not args.no_download:
        download_months(months, log=print)
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
            f"{built['quantity'].sum():,} departures) -> {path}"
        )


if __name__ == "__main__":
    main()
