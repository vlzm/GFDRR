"""Raw Citi Bike loaders and the ``RawModelData`` container.

Reads the raw trip CSV, then derives the raw entity tables (stations, depots,
trucks, bikes) with their capacities, costs and rates. ``RawModelData`` runs
all of this once and exposes the results as attributes; ``ResolvedModelData``
(in :mod:`dataloader_graph`) consumes it.

The trip CSV is the only external data in the project, so it gets an explicit
schema (:data:`TRIPS_SCHEMA`): the loaded table is checked once, at load time,
and a bad CSV fails here with a full list of violations instead of surfacing
later as an unrelated pandas error or a run-end invariant violation.

Loading keeps a processed copy of each CSV in ``data/processed/`` (Notations.md
§15): the first load parses and cleans the CSV and writes the result as
parquet; later loads read the parquet, which is much faster. The folder is a
cache — deleting it is always safe, the next load rebuilds it.
"""

import pathlib

import numpy as np
import pandas as pd
import pandera.pandas as pa

from gbp.model.journal_schema import schema_violations

#: Loose bounding box around the service area (New York City and Jersey City).
#: Wide enough that every real station fits with room to spare; a coordinate
#: outside it is a data error, not a new station.
SERVICE_AREA_LAT = (40.4, 41.1)
SERVICE_AREA_LNG = (-74.5, -73.4)


def in_service_area(trips_df: pd.DataFrame) -> pd.Series:
    """Mark the rows whose both endpoints lie inside the service-area box.

    Most published months carry a handful of rows at test docks or with
    plainly wrong coordinates (zeros, another city). Those rows are data
    errors; the loaders drop them the same way they drop rows with missing
    key fields.
    """
    lat_lo, lat_hi = SERVICE_AREA_LAT
    lng_lo, lng_hi = SERVICE_AREA_LNG
    inside = pd.Series(True, index=trips_df.index)
    for column in ("start_lat", "end_lat"):
        inside &= trips_df[column].between(lat_lo, lat_hi)
    for column in ("start_lng", "end_lng"):
        inside &= trips_df[column].between(lng_lo, lng_hi)
    return inside


def _trip_time_ordered(trips: pd.DataFrame) -> pd.Series:
    """Each trip ends at or after its start: ``started_at <= ended_at``."""
    return trips["started_at"] <= trips["ended_at"]


#: The key fields a trip row must have; rows missing any of them are dropped.
KEY_FIELDS = [
    "started_at",
    "ended_at",
    "start_station_id",
    "end_station_id",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
]


def clean_trips(trips_df: pd.DataFrame) -> pd.DataFrame:
    """Drop the rows no consumer of the trips table can use.

    Three groups go, each a data error in the published file, not a broken
    file: rows with a missing key field (:data:`KEY_FIELDS`); rows with an
    endpoint outside the service area (:func:`in_service_area`); and trips
    that end before they start. The last group appears once a year: on the
    fall-back night of daylight saving time the published wall-clock
    timestamps repeat one hour, so a trip riding across the clock change
    looks reversed. The timestamps carry no timezone marker, so the true
    order cannot be recovered.

    Every loader that parses a trip CSV calls this one function — old and new
    schema alike — so the cleaning rules cannot drift apart.
    """
    trips_df = trips_df.dropna(subset=KEY_FIELDS)
    trips_df = trips_df[in_service_area(trips_df)]
    trips_df = trips_df[_trip_time_ordered(trips_df)]
    return trips_df.reset_index(drop=True)


#: Schema of the loaded trips table: the columns the pipeline reads, their
#: dtypes (as fixed by :func:`load_trips_raw_df`), coordinates inside the
#: service area, and trip times in order. Extra CSV columns are allowed.
TRIPS_SCHEMA = pa.DataFrameSchema(
    columns={
        "ride_id": pa.Column("string", nullable=True),
        "rideable_type": pa.Column("string", nullable=False),
        "started_at": pa.Column("datetime64[ns]", nullable=False),
        "ended_at": pa.Column("datetime64[ns]", nullable=False),
        "start_station_id": pa.Column("string", nullable=False),
        "end_station_id": pa.Column("string", nullable=False),
        "start_lat": pa.Column("float64", pa.Check.in_range(*SERVICE_AREA_LAT), nullable=False),
        "start_lng": pa.Column("float64", pa.Check.in_range(*SERVICE_AREA_LNG), nullable=False),
        "end_lat": pa.Column("float64", pa.Check.in_range(*SERVICE_AREA_LAT), nullable=False),
        "end_lng": pa.Column("float64", pa.Check.in_range(*SERVICE_AREA_LNG), nullable=False),
    },
    checks=[
        pa.Check(
            _trip_time_ordered,
            name="started_at <= ended_at",
            error="a trip ends before it starts",
        )
    ],
    strict=False,
    name="trips",
)


# ---------------------------------------------------------------------------
# Raw loaders
# ---------------------------------------------------------------------------
def processed_trips_path(trips_path: str) -> pathlib.Path:
    """Where the processed copy of one trip CSV lives.

    ``<data folder>/processed/<csv name>.parquet`` — the ``processed`` folder
    sits next to the folder the CSV is in, so ``data/raw/x.csv`` maps to
    ``data/processed/x.parquet``.
    """
    csv = pathlib.Path(trips_path).resolve()
    return csv.parent.parent / "processed" / (csv.stem + ".parquet")


def load_trips_raw_df(trips_path: str) -> pd.DataFrame:
    """Load one raw trip CSV, using its processed parquet copy when it is fresh.

    The first load parses the CSV, drops the unusable rows
    (:func:`clean_trips`), and
    writes the cleaned table to ``data/processed/<csv name>.parquet``
    (see :func:`processed_trips_path`). Later loads read that parquet copy
    instead, which is much faster than parsing the CSV. The copy counts as
    fresh while it is newer than its CSV; delete the ``processed`` folder to
    force a rebuild — for example after changing the cleaning code here.

    The schema check (:data:`TRIPS_SCHEMA`) runs on every load, whichever
    file was read; it raises ``ValueError`` with every violation found. The
    processed copy is written only after the check passes, so a bad table is
    never cached.
    """
    csv = pathlib.Path(trips_path)
    processed = processed_trips_path(trips_path)
    processed_is_fresh = processed.exists() and processed.stat().st_mtime >= csv.stat().st_mtime
    if processed_is_fresh:
        trips_df = pd.read_parquet(processed)
    else:
        trips_dtypes = {
            "ride_id": "string",
            "rideable_type": "string",
            "start_station_name": "string",
            "start_station_id": "string",
            "end_station_name": "string",
            "end_station_id": "string",
            "start_lat": "float64",
            "start_lng": "float64",
            "end_lat": "float64",
            "end_lng": "float64",
            "member_casual": "string",
        }
        trips_df = pd.read_csv(
            trips_path,
            dtype=trips_dtypes,
            parse_dates=["started_at", "ended_at"],
        )
        trips_df = clean_trips(trips_df)
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


def get_stations(trips: pd.DataFrame) -> pd.DataFrame:
    """Build the unique station table from trip start and end points."""
    parts = []
    for p in ("start", "end"):
        cols = {f"{p}_station_id": "station_id", f"{p}_lat": "lat", f"{p}_lng": "lng"}
        parts.append(trips[list(cols)].rename(columns=cols))

    return (
        pd.concat(parts)
        .dropna(subset=["station_id"])
        .drop_duplicates("station_id")
        .reset_index(drop=True)
    )


def get_stations_costs(stations_df: pd.DataFrame) -> pd.DataFrame:
    """Return the fixed cost of each station (zero for all stations)."""
    return stations_df[["station_id"]].assign(fixed_cost_station=0)


def get_depots(rng: np.random.Generator, n: int) -> pd.DataFrame:
    """Synthesize ``n`` depots at random coordinates within the city box."""
    return pd.DataFrame(
        {
            "depot_id": [f"depot_{i + 1}" for i in range(n)],
            "lat": rng.uniform(40.68, 40.86, size=n),
            "lng": rng.uniform(-74.03, -73.90, size=n),
        }
    )


def get_depots_capacities(
    rng: np.random.Generator, depot_capacity: int, depots_df: pd.DataFrame
) -> pd.DataFrame:
    """Assign each depot a random capacity around ``depot_capacity``."""
    return depots_df[["depot_id"]].assign(
        capacity=rng.integers(
            int(depot_capacity * 0.75), int(depot_capacity * 1.25), size=len(depots_df)
        )
    )


def get_depots_costs(rng: np.random.Generator, depots_df: pd.DataFrame) -> pd.DataFrame:
    """Assign each depot a random fixed cost."""
    return depots_df[["depot_id"]].assign(
        fixed_cost_depot=np.round(rng.uniform(80.0, 200.0, size=len(depots_df)), 2)
    )


def get_trips_df(trips_raw_df: pd.DataFrame) -> pd.DataFrame:
    """Select the trip columns the rest of the pipeline needs."""
    cols = [
        "ride_id",
        "rideable_type",
        "started_at",
        "ended_at",
        "start_station_name",
        "start_station_id",
        "end_station_name",
        "end_station_id",
    ]
    return trips_raw_df[cols].copy()


def get_trucks_df(n_trucks: int) -> pd.DataFrame:
    """Build a table of ``n_trucks`` trucks with generated ids.

    Every truck starts at the first depot (``home_depot_id = "depot_1"``);
    a run can replace the fleet with
    :func:`gbp.loaders.dataloader_graph.apply_truck_fleet`.
    """
    truck_ids = [f"truck_{i + 1}" for i in range(n_trucks)]
    return pd.DataFrame({"truck_id": truck_ids, "home_depot_id": "depot_1"})


def get_trucks_rates_df(truck_rate: float, df_trucks: pd.DataFrame) -> pd.DataFrame:
    """Assign the same per-unit rate to every truck."""
    return pd.DataFrame({"truck_id": df_trucks["truck_id"], "rate": truck_rate})


def get_trucks_capacities_df(truck_capacity_bikes: int, df_trucks: pd.DataFrame) -> pd.DataFrame:
    """Assign the same bike capacity to every truck."""
    return pd.DataFrame({"truck_id": df_trucks["truck_id"], "capacity": truck_capacity_bikes})


def get_bike_rates_df(electric_bike_rate: float, classic_bike_rate: float) -> pd.DataFrame:
    """Build the per-unit rate table for the two bike commodities."""
    return pd.DataFrame(
        {
            "rideable_type": ["electric_bike", "classic_bike"],
            "rate": [electric_bike_rate, classic_bike_rate],
        }
    )


# ---------------------------------------------------------------------------
# Raw model data container
# ---------------------------------------------------------------------------
class RawModelData:
    """Raw entity tables for one scenario, loaded once from data sources.

    Parameters
    ----------
    trips_path : str
        Path to the raw Citi Bike trip CSV.
    seed : int
        Seed for the random generator used to synthesize depots and trucks.
    n_depots, depot_capacity, n_trucks, truck_capacity_bikes : int
        Sizing of the synthetic depot and truck fleet.
    truck_rate, electric_bike_rate, classic_bike_rate : float
        Per-unit rates for trucks and the two bike commodities.
    """

    def __init__(
        self,
        trips_path: str,
        seed: int,
        n_depots: int,
        depot_capacity: int,
        n_trucks: int,
        truck_capacity_bikes: int,
        truck_rate: float,
        electric_bike_rate: float,
        classic_bike_rate: float,
    ) -> None:
        self.rng = np.random.default_rng(seed=seed)

        # Raw sources. The path is kept so a saved run can record which raw
        # file it was built from (the ``inputs`` field of ``meta.json``).
        self.trips_path = trips_path
        self.trips_raw_df = load_trips_raw_df(trips_path)

        # Stations. Real dock capacities are not loaded; every station gets the
        # constant capacity 100 (the sizing run replaces it anyway).
        self.stations_df = get_stations(self.trips_raw_df)
        self.stations_capacities_df = self.stations_df[["station_id"]].assign(capacity=100)
        self.stations_costs_df = get_stations_costs(self.stations_df)

        # Depots
        self.depots_df = get_depots(self.rng, n_depots)
        self.depot_capacities_df = get_depots_capacities(self.rng, depot_capacity, self.depots_df)
        self.depot_costs_df = get_depots_costs(self.rng, self.depots_df)

        # Trips
        self.trips_df = get_trips_df(self.trips_raw_df)

        # Trucks
        self.trucks_df = get_trucks_df(n_trucks)
        self.trucks_rates_df = get_trucks_rates_df(truck_rate, self.trucks_df)
        self.trucks_capacities_df = get_trucks_capacities_df(truck_capacity_bikes, self.trucks_df)

        # Bikes
        self.bike_rates_df = get_bike_rates_df(electric_bike_rate, classic_bike_rate)
