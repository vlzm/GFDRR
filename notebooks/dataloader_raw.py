import pandas as pd
import numpy as np
import requests


# ---------------------------------------------------------------------------
# Raw loaders
# ---------------------------------------------------------------------------
def load_trips_raw_df(trips_path: str) -> pd.DataFrame:
    trips_dtypes = {
        'ride_id': 'string',
        'rideable_type': 'string',
        'start_station_name': 'string',
        'start_station_id': 'string',
        'end_station_name': 'string',
        'end_station_id': 'string',
        'start_lat': 'float64',
        'start_lng': 'float64',
        'end_lat': 'float64',
        'end_lng': 'float64',
        'member_casual': 'string',
    }
    trips_df = pd.read_csv(
        trips_path,
        dtype=trips_dtypes,
        parse_dates=['started_at', 'ended_at'],
    )
    trips_df = trips_df.dropna(subset=[
        'started_at', 'ended_at',
        'start_station_id', 'end_station_id',
        'start_lat', 'start_lng', 'end_lat', 'end_lng',
    ])
    trips_df = trips_df.reset_index(drop=True)
    return trips_df


def load_gbfs_raw_df(gbfs_base: str) -> pd.DataFrame:
    station_info = requests.get(f"{gbfs_base}/station_information.json").json()
    station_status = requests.get(f"{gbfs_base}/station_status.json").json()
    info_df = pd.DataFrame(station_info["data"]["stations"])
    status_df = pd.DataFrame(station_status["data"]["stations"])
    stations = info_df.merge(status_df, on="station_id")
    return stations


def get_stations(trips: pd.DataFrame) -> pd.DataFrame:
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


def get_stations_capacities(gbfs: pd.DataFrame, stations_df: pd.DataFrame) -> pd.DataFrame:
    return (
        gbfs.query("is_installed == 1")[["short_name", "capacity"]]
        .drop_duplicates("short_name")
        .rename(columns={"short_name": "station_id"})
        .query("station_id in @stations_df.station_id")
        .reset_index(drop=True)
    )


def get_stations_costs(stations_df: pd.DataFrame) -> pd.DataFrame:
    return stations_df[["station_id"]].assign(fixed_cost_station=0)


def get_depots(rng: np.random.Generator, n: int) -> pd.DataFrame:
    return pd.DataFrame({
        "depot_id": [f"depot_{i + 1}" for i in range(n)],
        "lat":     rng.uniform(40.68, 40.86, size=n),
        "lng":     rng.uniform(-74.03, -73.90, size=n),
    })


def get_depots_capacities(rng: np.random.Generator, depot_capacity: int, depots_df: pd.DataFrame) -> pd.DataFrame:
    return depots_df[["depot_id"]].assign(
        capacity=rng.integers(
            int(depot_capacity * 0.75), int(depot_capacity * 1.25), size=len(depots_df)
        )
    )


def get_depots_costs(rng: np.random.Generator, depots_df: pd.DataFrame) -> pd.DataFrame:
    return depots_df[["depot_id"]].assign(
        fixed_cost_depot=np.round(rng.uniform(80.0, 200.0, size=len(depots_df)), 2)
    )


def get_initial_inventory_df(gbfs_raw: pd.DataFrame, stations_df: pd.DataFrame) -> pd.DataFrame:
    """Initial inventory split by commodity (classic vs electric).

    GBFS ``num_bikes_available`` is the total available count; ``num_ebikes_available``
    is the electric subset, so ``classic = total - ebikes``. If the electric
    field is absent, everything is booked as classic (and you should then keep
    user flows to classic-only, or electric trips will depart from zero stock).
    Verify the field semantics against the actual payload columns.
    """
    g = (
        gbfs_raw.query("is_installed == 1")
        .rename(columns={"short_name": "facility_id"})
        .query("facility_id in @stations_df.station_id")
    )
    ebikes = g["num_ebikes_available"] if "num_ebikes_available" in g.columns else 0
    classic = g["num_bikes_available"] - ebikes
    return pd.concat([
        pd.DataFrame({"facility_id": g["facility_id"], "commodity_category": "classic_bike",  "quantity": classic}),
        pd.DataFrame({"facility_id": g["facility_id"], "commodity_category": "electric_bike", "quantity": ebikes}),
    ], ignore_index=True).reset_index(drop=True)


def get_trips_df(trips_raw_df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        'ride_id', 'rideable_type', 'started_at', 'ended_at',
        'start_station_name', 'start_station_id',
        'end_station_name', 'end_station_id',
    ]
    return trips_raw_df[cols].copy()


def get_trucks_df(n_trucks: int) -> pd.DataFrame:
    truck_ids = [f"truck_{i + 1}" for i in range(n_trucks)]
    return pd.DataFrame({"truck_id": truck_ids})


def get_trucks_rates_df(truck_rate: float, df_trucks: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({"truck_id": df_trucks["truck_id"], "rate": truck_rate})


def get_trucks_capacities_df(truck_capacity_bikes: int, df_trucks: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({"truck_id": df_trucks["truck_id"], "capacity": truck_capacity_bikes})


def get_bike_rates_df(electric_bike_rate: float, classic_bike_rate: float) -> pd.DataFrame:
    return pd.DataFrame({
        "rideable_type": ["electric_bike", "classic_bike"],
        "rate": [electric_bike_rate, classic_bike_rate],
    })


# ---------------------------------------------------------------------------
# Config + execution
# ---------------------------------------------------------------------------
trips_path = "/Users/vladislav/Documents/vlzm/GFDRR/data/raw/202601-citibike-tripdata_1.csv"
# trips_path = '/mnt/outer/Documents/vlzm/GFDRR/data/raw/202602-citibike-tripdata_1.csv'
# trips_path = 'D:/Users/vladislav/Documents/vlzm/GFDRR/data/raw/202601-citibike-tripdata_1.csv'

config = {
    "gbfs_base": "https://gbfs.citibikenyc.com/gbfs/en",
    "trips_path": trips_path,
    "seed": 42,
    "n_depots": 10,
    "depot_capacity": 9000,
    "n_trucks": 5,
    "truck_capacity_bikes": 20,
    "truck_rate": 50.0,
    "electric_bike_rate": 5,
    "classic_bike_rate": 3,
}
_rng = np.random.default_rng(seed=config["seed"])
config["rng"] = _rng

trips_raw_df = load_trips_raw_df(config["trips_path"])
gbfs_raw_df = load_gbfs_raw_df(config["gbfs_base"])

stations_df = get_stations(trips_raw_df)
stations_capacities_df = get_stations_capacities(gbfs_raw_df, stations_df)
stations_costs_df = get_stations_costs(stations_df)

depots_df = get_depots(config["rng"], config["n_depots"])
depot_capacities_df = get_depots_capacities(config["rng"], config["depot_capacity"], depots_df)
depot_costs_df = get_depots_costs(config["rng"], depots_df)

trips_df = get_trips_df(trips_raw_df)

trucks_df = get_trucks_df(config["n_trucks"])
trucks_rates_df = get_trucks_rates_df(config["truck_rate"], trucks_df)
trucks_capacities_df = get_trucks_capacities_df(config["truck_capacity_bikes"], trucks_df)

bike_rates_df = get_bike_rates_df(config["electric_bike_rate"], config["classic_bike_rate"])