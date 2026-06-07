"""Raw Citi Bike loader.

Public interface::

    RawModelConfig   — parameters for loading
    RawModelData     — immutable result container
    load_raw_model   — the single entry point

Everything prefixed with ``_`` is private implementation.

Example::

    config = RawModelConfig(
        gbfs_base="https://gbfs.citibikenyc.com/gbfs/en",
        trips_path="data/202301-citibike-tripdata.csv",
    )
    raw = load_raw_model(config)
    raw.stations  # DataFrame of unique stations

Network access
    The loader makes HTTP GET requests to the GBFS feed at
    ``config.gbfs_base``.  No network → ``requests.ConnectionError``.

File access
    The trip CSV at ``config.trips_path`` must exist and be readable.
    Missing file → ``FileNotFoundError``.
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd
import requests

__all__ = ["RawModelConfig", "RawModelData", "load_raw_model"]


# ═══════════════════════════════════════════════════════════════════════════
# Public interface
# ═══════════════════════════════════════════════════════════════════════════


@dataclass(frozen=True)
class RawModelConfig:
    """All parameters needed to load a raw model.

    Only ``gbfs_base`` and ``trips_path`` are required.
    Everything else has sensible defaults for a small-scale scenario.

    Parameters
    ----------
    gbfs_base : str
        Base URL of the GBFS feed.  Must serve
        ``/station_information.json`` and ``/station_status.json``.
    trips_path : str
        Path to the raw Citi Bike trip CSV (one month).
    seed : int
        Random seed for reproducible depot/truck generation.
        Same seed + same config → identical RawModelData.
    n_depots : int
        Number of synthetic depots to generate.
    depot_capacity : int
        Base depot capacity.  Actual values randomized to ±25%.
    n_trucks : int
        Number of synthetic trucks.
    truck_capacity_bikes : int
        How many bikes one truck carries.
    truck_rate : float
        Cost per truck per trip.
    electric_bike_rate, classic_bike_rate : float
        Cost per bike-minute for each commodity type.
    """

    gbfs_base: str
    trips_path: str
    seed: int = 42
    n_depots: int = 3
    depot_capacity: int = 100
    n_trucks: int = 5
    truck_capacity_bikes: int = 20
    truck_rate: float = 50.0
    electric_bike_rate: float = 0.26
    classic_bike_rate: float = 0.18


@dataclass(frozen=True)
class RawModelData:
    """Immutable container of raw entity tables.

    Every field is a pandas DataFrame.  The container holds data only —
    no methods, no I/O, no validation logic.

    All DataFrames are guaranteed non-empty after a successful
    ``load_raw_model`` call (the loader drops incomplete rows
    at source, but never produces empty tables for valid inputs).

    Fields
    ------
    stations : DataFrame
        Columns: ``station_id``, ``lat``, ``lng``.
        Unique stations extracted from trip start/end points.
    stations_capacities : DataFrame
        Columns: ``station_id``, ``capacity``.
        Bike dock count from GBFS (installed stations only).
    stations_costs : DataFrame
        Columns: ``station_id``, ``fixed_cost_station``.
        Currently hardcoded to 0 — placeholder for future costing.
    depots : DataFrame
        Columns: ``depot_id``, ``lat``, ``lng``.
        Randomly generated within NYC bounding box.
    depot_capacities : DataFrame
        Columns: ``depot_id``, ``capacity``.
        Randomized ±25 % around ``config.depot_capacity``.
    depot_costs : DataFrame
        Columns: ``depot_id``, ``fixed_cost_depot``.
        Uniform [80, 200] per depot.
    trips : DataFrame
        Columns: ``ride_id``, ``rideable_type``, ``started_at``,
        ``ended_at``, ``start_station_name``, ``start_station_id``,
        ``end_station_name``, ``end_station_id``.
        Rows with missing station or coordinate data are already dropped.
    trucks : DataFrame
        Columns: ``truck_id``.
    trucks_rates : DataFrame
        Columns: ``truck_id``, ``rate``.
    trucks_capacities : DataFrame
        Columns: ``truck_id``, ``capacity``.
    bike_rates : DataFrame
        Columns: ``rideable_type``, ``rate``.
        Exactly two rows: ``electric_bike`` and ``classic_bike``.
    initial_inventory : DataFrame
        Columns: ``facility_id``, ``commodity_category``, ``quantity``.
        Split into ``classic_bike`` / ``electric_bike`` from GBFS counts.
    """

    stations: pd.DataFrame
    stations_capacities: pd.DataFrame
    stations_costs: pd.DataFrame
    depots: pd.DataFrame
    depot_capacities: pd.DataFrame
    depot_costs: pd.DataFrame
    trips: pd.DataFrame
    trucks: pd.DataFrame
    trucks_rates: pd.DataFrame
    trucks_capacities: pd.DataFrame
    bike_rates: pd.DataFrame
    initial_inventory: pd.DataFrame


def load_raw_model(config: RawModelConfig) -> RawModelData:
    """Load raw bike-sharing data and derive all entity tables.

    Performs network I/O (GBFS feed) and file I/O (trip CSV), then
    synthesises depots and trucks deterministically from ``config.seed``.

    Parameters
    ----------
    config : RawModelConfig
        Loading parameters.  See class docstring for field details.

    Returns
    -------
    RawModelData
        Frozen container with all entity tables populated.

    Raises
    ------
    requests.ConnectionError
        If the GBFS feed at ``config.gbfs_base`` is unreachable.
    FileNotFoundError
        If ``config.trips_path`` does not exist.
    KeyError
        If the GBFS JSON payload is missing expected fields
        (``station_id``, ``capacity``, ``num_bikes_available``).

    Notes
    -----
    Determinism guarantee: identical ``config`` (including ``seed``)
    always produces identical ``RawModelData``.  Non-determinism can
    only come from the GBFS feed returning different data between calls.
    """
    rng = np.random.default_rng(seed=config.seed)

    trips_raw = _load_trips_csv(config.trips_path)
    gbfs_raw = _load_gbfs_feed(config.gbfs_base)

    stations = _extract_stations(trips_raw)
    depots = _generate_depots(rng, config.n_depots)
    trucks = _generate_trucks(config.n_trucks)

    return RawModelData(
        stations=stations,
        stations_capacities=_station_capacities(gbfs_raw, stations),
        stations_costs=stations[["station_id"]].assign(fixed_cost_station=0),
        depots=depots,
        depot_capacities=_depot_capacities(rng, config.depot_capacity, depots),
        depot_costs=_depot_costs(rng, depots),
        trips=_select_trip_columns(trips_raw),
        trucks=trucks,
        trucks_rates=_truck_rates(config.truck_rate, trucks),
        trucks_capacities=_truck_capacities(config.truck_capacity_bikes, trucks),
        bike_rates=_bike_rates(config.electric_bike_rate, config.classic_bike_rate),
        initial_inventory=_initial_inventory(gbfs_raw, stations),
    )


# ═══════════════════════════════════════════════════════════════════════════
# Private implementation — nothing below is part of the interface
# ═══════════════════════════════════════════════════════════════════════════


def _load_trips_csv(path: str) -> pd.DataFrame:
    dtypes = {
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
    df = pd.read_csv(path, dtype=dtypes, parse_dates=["started_at", "ended_at"])
    df = df.dropna(subset=[
        "started_at", "ended_at",
        "start_station_id", "end_station_id",
        "start_lat", "start_lng", "end_lat", "end_lng",
    ])
    return df.reset_index(drop=True)


def _load_gbfs_feed(base_url: str) -> pd.DataFrame:
    info = requests.get(f"{base_url}/station_information.json").json()
    status = requests.get(f"{base_url}/station_status.json").json()
    info_df = pd.DataFrame(info["data"]["stations"])
    status_df = pd.DataFrame(status["data"]["stations"])
    return info_df.merge(status_df, on="station_id")


def _extract_stations(trips: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for prefix in ("start", "end"):
        cols = {
            f"{prefix}_station_id": "station_id",
            f"{prefix}_lat": "lat",
            f"{prefix}_lng": "lng",
        }
        parts.append(trips[list(cols)].rename(columns=cols))
    return (
        pd.concat(parts)
        .dropna(subset=["station_id"])
        .drop_duplicates("station_id")
        .reset_index(drop=True)
    )


def _station_capacities(
    gbfs: pd.DataFrame, stations: pd.DataFrame,
) -> pd.DataFrame:
    return (
        gbfs.query("is_installed == 1")[["short_name", "capacity"]]
        .drop_duplicates("short_name")
        .rename(columns={"short_name": "station_id"})
        .query("station_id in @stations.station_id")
        .reset_index(drop=True)
    )


def _generate_depots(rng: np.random.Generator, n: int) -> pd.DataFrame:
    return pd.DataFrame({
        "depot_id": [f"depot_{i + 1}" for i in range(n)],
        "lat": rng.uniform(40.68, 40.86, size=n),
        "lng": rng.uniform(-74.03, -73.90, size=n),
    })


def _depot_capacities(
    rng: np.random.Generator, base_capacity: int, depots: pd.DataFrame,
) -> pd.DataFrame:
    return depots[["depot_id"]].assign(
        capacity=rng.integers(
            int(base_capacity * 0.75),
            int(base_capacity * 1.25),
            size=len(depots),
        ),
    )


def _depot_costs(
    rng: np.random.Generator, depots: pd.DataFrame,
) -> pd.DataFrame:
    return depots[["depot_id"]].assign(
        fixed_cost_depot=np.round(rng.uniform(80.0, 200.0, size=len(depots)), 2),
    )


def _select_trip_columns(trips_raw: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "ride_id", "rideable_type", "started_at", "ended_at",
        "start_station_name", "start_station_id",
        "end_station_name", "end_station_id",
    ]
    return trips_raw[cols].copy()


def _generate_trucks(n: int) -> pd.DataFrame:
    return pd.DataFrame({"truck_id": [f"truck_{i + 1}" for i in range(n)]})


def _truck_rates(rate: float, trucks: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({"truck_id": trucks["truck_id"], "rate": rate})


def _truck_capacities(capacity: int, trucks: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({"truck_id": trucks["truck_id"], "capacity": capacity})


def _bike_rates(electric_rate: float, classic_rate: float) -> pd.DataFrame:
    return pd.DataFrame({
        "rideable_type": ["electric_bike", "classic_bike"],
        "rate": [electric_rate, classic_rate],
    })


def _initial_inventory(
    gbfs: pd.DataFrame, stations: pd.DataFrame,
) -> pd.DataFrame:
    g = (
        gbfs.query("is_installed == 1")
        .rename(columns={"short_name": "facility_id"})
        .query("facility_id in @stations.station_id")
    )
    ebikes = g["num_ebikes_available"] if "num_ebikes_available" in g.columns else 0
    classic = g["num_bikes_available"] - ebikes
    return pd.concat(
        [
            pd.DataFrame({
                "facility_id": g["facility_id"],
                "commodity_category": "classic_bike",
                "quantity": classic,
            }),
            pd.DataFrame({
                "facility_id": g["facility_id"],
                "commodity_category": "electric_bike",
                "quantity": ebikes,
            }),
        ],
        ignore_index=True,
    )