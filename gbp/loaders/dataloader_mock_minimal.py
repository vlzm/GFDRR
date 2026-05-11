"""Minimal Citi Bike-style mock — ``df_stations`` + ``df_trips`` only.

``df_trips`` mirrors the 13-column schema of a real Citi Bike CSV (see
``data/raw/202602-citibike-tripdata_*.csv``): ``ride_id``,
``rideable_type``, ``started_at``, ``ended_at``, ``start_station_name``,
``start_station_id``, ``end_station_name``, ``end_station_id``,
``start_lat``, ``start_lng``, ``end_lat``, ``end_lng``, ``member_casual``.

``df_stations`` carries ``station_id``, ``station_name``, ``lat``, ``lon``
(``lon`` rather than ``lng`` to satisfy ``StationsSourceSchema``).  All
other ``BikeShareSourceProtocol`` fields are ``None`` so ``build_model``
can derive periods, facility_roles, categories, demand/supply, and the
initial inventory from the generated trips.

Usage::

    from gbp.build.pipeline import build_model
    from gbp.loaders import DataLoaderGraph, DataLoaderMockMinimal

    mock = DataLoaderMockMinimal({"n_stations": 10, "n_trips": 200})
    raw = DataLoaderGraph(mock).load()
    resolved = build_model(raw)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

RIDEABLE_TYPES: tuple[str, ...] = ("electric_bike", "classic_bike")
MEMBER_CASUAL: tuple[str, ...] = ("member", "casual")
STATION_NAME_POOL: tuple[str, ...] = (
    "Bond St & Bergen St",
    "Amity St & Court St",
    "Norman Ave & Leonard St",
    "Jackson Ave & 49 Ave",
    "E 2 St & Ave C",
    "E 2 St & 2 Ave",
    "St James Pl & Oliver St",
    "W 4 St & 7 Ave S",
    "Broadway & W 51 St",
    "Grand Army Plaza & Central Park S",
    "Front St & Gold St",
    "Henry St & Grand St",
)

TRIP_COLUMNS: tuple[str, ...] = (
    "ride_id",
    "rideable_type",
    "started_at",
    "ended_at",
    "start_station_name",
    "start_station_id",
    "end_station_name",
    "end_station_id",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "member_casual",
)
STATION_COLUMNS: tuple[str, ...] = ("station_id", "station_name", "lat", "lon")


class DataLoaderMockMinimal:
    """Generate a minimal Citi Bike-style source: stations + trips only.

    All other ``BikeShareSourceProtocol`` fields are ``None`` so
    ``build_model`` can derive periods, facility_roles, categories,
    demand/supply, and initial inventory from the generated trips.

    Parameters
    ----------
    config
        All keys are optional:

        - ``n_stations`` -- number of stations (default 8).
        - ``n_trips`` -- total trips to generate (default 200).
        - ``n_days`` -- span of the trip history in days (default 3).
        - ``start_date`` -- first day of the history (default ``"2025-01-01"``).
        - ``seed`` -- RNG seed for reproducibility (default 42).
        - ``lat_range`` / ``lon_range`` -- bounding box for station coordinates
          (defaults roughly cover lower Manhattan).
        - ``min_trip_minutes`` / ``max_trip_minutes`` -- trip duration range
          used to synthesize ``ended_at`` (defaults 3 / 45).
    """

    def __init__(self, config: dict | None = None) -> None:
        self.config = config or {}
        self._rng = np.random.default_rng(seed=self.config.get("seed", 42))

        # Optional BikeShareSourceProtocol fields — always None in minimal mode.
        # Declaring them here lets the class satisfy the protocol via attribute
        # presence, and downstream code can use plain ``getattr(..., None)``.
        self.df_depots: pd.DataFrame | None = None
        self.df_resources: pd.DataFrame | None = None
        self.df_station_capacities: pd.DataFrame | None = None
        self.df_depot_capacities: pd.DataFrame | None = None
        self.df_resource_capacities: pd.DataFrame | None = None
        self.timestamps: pd.DatetimeIndex | None = None
        self.inventory_initial: pd.DataFrame | None = None
        self.df_telemetry_ts: pd.DataFrame | None = None
        self.df_station_costs: pd.DataFrame | None = None
        self.df_depot_costs: pd.DataFrame | None = None
        self.df_truck_rates: pd.DataFrame | None = None

        self.df_stations: pd.DataFrame = pd.DataFrame(columns=list(STATION_COLUMNS))
        self.df_trips: pd.DataFrame = pd.DataFrame(columns=list(TRIP_COLUMNS))

    def load_data(self) -> None:
        """Generate stations and Citi Bike-shaped trips between them.

        Populates ``df_stations``, ``df_trips``, ``df_resources``, and
        ``df_resource_capacities`` on the instance.
        """
        n_stations = int(self.config.get("n_stations", 8))
        n_trips = int(self.config.get("n_trips", 200))
        n_days = int(self.config.get("n_days", 3))
        start_date = pd.Timestamp(self.config.get("start_date", "2025-01-01"))
        lat_lo, lat_hi = self.config.get("lat_range", (40.70, 40.80))
        lon_lo, lon_hi = self.config.get("lon_range", (-74.02, -73.92))
        min_trip = int(self.config.get("min_trip_minutes", 3))
        max_trip = int(self.config.get("max_trip_minutes", 45))

        # Stations — real Citi Bike ids look like "4404.10"; we mimic that shape.
        station_ids = np.array(
            [f"{4000 + i * 13}.{(i % 20) + 1:02d}" for i in range(n_stations)]
        )
        station_names = np.array(
            [STATION_NAME_POOL[i % len(STATION_NAME_POOL)] for i in range(n_stations)]
        )
        station_lats = self._rng.uniform(lat_lo, lat_hi, size=n_stations)
        station_lons = self._rng.uniform(lon_lo, lon_hi, size=n_stations)

        self.df_stations = pd.DataFrame({
            "station_id": station_ids,
            "station_name": station_names,
            "lat": station_lats,
            "lon": station_lons,
        })

        # Trip endpoints (ensure start != end).
        start_idx = self._rng.integers(0, n_stations, size=n_trips)
        end_idx = (start_idx + self._rng.integers(1, n_stations, size=n_trips)) % n_stations

        # Timestamps vectorized via pd.to_timedelta.
        offset_minutes = self._rng.integers(0, n_days * 24 * 60, size=n_trips)
        duration_minutes = self._rng.integers(min_trip, max_trip + 1, size=n_trips)
        started_at = start_date + pd.to_timedelta(offset_minutes, unit="m")
        ended_at = started_at + pd.to_timedelta(duration_minutes, unit="m")

        # 16-char uppercase hex ride_ids, matching the CSV shape.
        hi = self._rng.integers(0, 2**32, size=n_trips, dtype=np.uint32)
        lo = self._rng.integers(0, 2**32, size=n_trips, dtype=np.uint32)
        ride_ids = [f"{int(h):08X}{int(l):08X}" for h, l in zip(hi, lo)]

        rideable_types = self._rng.choice(RIDEABLE_TYPES, size=n_trips)
        member_casual = self._rng.choice(MEMBER_CASUAL, size=n_trips)

        self.df_trips = pd.DataFrame({
            "ride_id": ride_ids,
            "rideable_type": rideable_types,
            "started_at": started_at,
            "ended_at": ended_at,
            "start_station_name": station_names[start_idx],
            "start_station_id": station_ids[start_idx],
            "end_station_name": station_names[end_idx],
            "end_station_id": station_ids[end_idx],
            "start_lat": station_lats[start_idx],
            "start_lng": station_lons[start_idx],
            "end_lat": station_lats[end_idx],
            "end_lng": station_lons[end_idx],
            "member_casual": member_casual,
        }).sort_values("started_at").reset_index(drop=True)

        # Resources — no capacity on the table
        num_resources = self.config.get("num_resources", 3)
        resource_cap = self.config.get("resource_capacity", 100)
        resource_ids = [f"truck_{i + 1}" for i in range(num_resources)]
        self.df_resources = pd.DataFrame({"resource_id": resource_ids})
        self.df_resource_capacities = pd.DataFrame({
            "resource_id": resource_ids,
            "capacity": [resource_cap] * num_resources,
        })
