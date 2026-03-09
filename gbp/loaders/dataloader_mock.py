from __future__ import annotations

import uuid

import numpy as np
import pandas as pd


class DataLoaderMock:
    """Generate Citi Bike-like temporal mock data for rebalancing tests.

    Required by ``DataSourceProtocol``:
        - df_stations     [node_id, inventory_capacity, lat, lon, ...extra metadata]
        - df_depots       [node_id, lat, lon]
        - df_resources    [resource_id, capacity]
        - timestamps      DatetimeIndex
        - df_inventory_ts wide DataFrame [index=timestamps, columns=station node_ids]

    Additional generated artifacts:
        - df_telemetry_ts [station status over time]
        - df_trips        [trip-level records]
        - df_station_costs
        - df_truck_rates
    """

    def __init__(self, config: dict):
        self.config = config
        self._rng = np.random.default_rng(seed=self.config.get("seed", 42))

    def load_data(self) -> None:
        n_stations = self.config["n"]
        n_depots = self.config.get("n_depots", 2)
        n_timestamps = self.config.get("n_timestamps", 168)
        start_date = self.config.get("start_date", "2025-01-01")
        freq = self.config.get("time_freq", "h")

        self.df_stations = self._generate_stations(n_stations)
        self.df_depots = self._generate_depots(n_depots)
        self.df_resources = self._generate_resources()
        self.timestamps = pd.date_range(start=start_date, periods=n_timestamps, freq=freq)

        self.df_inventory_ts, self.df_telemetry_ts, self.df_trips = self._generate_telemetry_and_trips(
            df_stations=self.df_stations,
            timestamps=self.timestamps,
        )

        self.df_station_costs, self.df_truck_rates = self._generate_costs(
            df_stations=self.df_stations,
            df_resources=self.df_resources,
        )

    # ------------------------------------------------------------------

    def _generate_stations(self, n: int) -> pd.DataFrame:
        station_ids = [f"station_{i + 1}" for i in range(n)]
        street_prefix = np.array(["W", "E"])
        avenues = np.array(["1 Ave", "2 Ave", "3 Ave", "5 Ave", "6 Ave", "7 Ave", "Broadway", "Park Ave"])

        street_num_1 = self._rng.integers(10, 95, size=n)
        street_num_2 = self._rng.integers(10, 95, size=n)
        dirs_1 = self._rng.choice(street_prefix, size=n)
        dirs_2 = self._rng.choice(street_prefix, size=n)
        ave = self._rng.choice(avenues, size=n)

        names = [f"{d1} {s1} St & {a}" for d1, s1, a in zip(dirs_1, street_num_1, ave, strict=False)]
        short_names = [f"{d2} {s2}" for d2, s2 in zip(dirs_2, street_num_2, strict=False)]

        return pd.DataFrame({
            "node_id": station_ids,
            "station_id": station_ids,
            "inventory_capacity": self._rng.integers(15, 60, size=n),
            "lat": self._rng.uniform(40.68, 40.86, size=n),
            "lon": self._rng.uniform(-74.03, -73.90, size=n),
            "name": names,
            "short_name": short_names,
            "region_id": self._rng.integers(1, 6, size=n).astype(str),
            "is_installed": 1,
            "is_renting": 1,
            "is_returning": 1,
        }).assign(capacity=lambda df: df["inventory_capacity"])

    def _generate_depots(self, n: int) -> pd.DataFrame:
        return pd.DataFrame({
            "node_id": [f"depot_{i + 1}" for i in range(n)],
            "lat": self._rng.uniform(40.68, 40.86, size=n),
            "lon": self._rng.uniform(-74.03, -73.90, size=n),
        })

    def _generate_resources(self) -> pd.DataFrame:
        num = self.config.get("num_resources", 3)
        cap = self.config.get("resource_capacity", 100)
        return pd.DataFrame({
            "resource_id": [f"truck_{i + 1}" for i in range(num)],
            "capacity": [cap] * num,
        })

    def _generate_telemetry_and_trips(
        self,
        df_stations: pd.DataFrame,
        timestamps: pd.DatetimeIndex,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        capacities = df_stations["inventory_capacity"].to_numpy(dtype=int)
        station_ids = df_stations["node_id"].to_numpy()
        n_stations = len(station_ids)
        n_steps = len(timestamps)

        ebike_fraction = float(self.config.get("ebike_fraction", 0.3))
        member_fraction = float(self.config.get("member_fraction", 0.7))
        avg_trip_duration_min = int(self.config.get("avg_trip_duration_min", 15))
        disabled_dock_prob = float(self.config.get("disabled_dock_prob", 0.05))
        trips_per_hour_per_station = float(self.config.get("trips_per_hour_per_station", 0.7))

        inventory = np.zeros((n_steps, n_stations), dtype=int)
        inventory[0] = self._rng.integers(low=0, high=capacities + 1)

        trip_records: list[dict] = []
        min_duration = max(5, avg_trip_duration_min - 10)
        max_duration = max(min_duration + 1, avg_trip_duration_min + 15)

        for t_idx in range(n_steps - 1):
            current = inventory[t_idx].copy()
            next_qty = current.copy()

            step_hours = max(
                (timestamps[t_idx + 1] - timestamps[t_idx]).total_seconds() / 3600.0,
                1.0,
            )
            estimated_trips = n_stations * trips_per_hour_per_station * step_hours
            n_trip_events = int(self._rng.poisson(lam=max(estimated_trips, 1.0)))

            for _ in range(n_trip_events):
                candidate_starts = np.where(next_qty > 0)[0]
                if len(candidate_starts) == 0:
                    break

                start_idx = int(self._rng.choice(candidate_starts))
                available_docks = capacities - next_qty
                candidate_ends = np.where((available_docks > 0) & (np.arange(n_stations) != start_idx))[0]
                if len(candidate_ends) == 0:
                    break

                end_idx = int(self._rng.choice(candidate_ends))
                next_qty[start_idx] -= 1
                next_qty[end_idx] += 1

                start_ts = pd.Timestamp(timestamps[t_idx]) + pd.Timedelta(
                    minutes=int(self._rng.integers(0, int(step_hours * 60))),
                )
                duration_min = int(self._rng.integers(min_duration, max_duration))
                end_ts = start_ts + pd.Timedelta(minutes=duration_min)

                rideable_type = (
                    "electric_bike"
                    if self._rng.random() < ebike_fraction
                    else "classic_bike"
                )
                member_casual = (
                    "member"
                    if self._rng.random() < member_fraction
                    else "casual"
                )

                trip_records.append({
                    "ride_id": str(uuid.uuid4()),
                    "rideable_type": rideable_type,
                    "started_at": start_ts,
                    "ended_at": end_ts,
                    "start_station_name": df_stations.iloc[start_idx]["name"],
                    "start_station_id": df_stations.iloc[start_idx]["node_id"],
                    "end_station_name": df_stations.iloc[end_idx]["name"],
                    "end_station_id": df_stations.iloc[end_idx]["node_id"],
                    "start_lat": float(df_stations.iloc[start_idx]["lat"]),
                    "start_lng": float(df_stations.iloc[start_idx]["lon"]),
                    "end_lat": float(df_stations.iloc[end_idx]["lat"]),
                    "end_lng": float(df_stations.iloc[end_idx]["lon"]),
                    "member_casual": member_casual,
                })

            inventory[t_idx + 1] = np.clip(next_qty, 0, capacities)

        df_inventory_ts = pd.DataFrame(inventory, index=timestamps, columns=station_ids)

        telemetry_records: list[dict] = []
        for t_idx, ts in enumerate(timestamps):
            bikes = inventory[t_idx]
            disabled_docks = self._rng.binomial(n=np.maximum(capacities // 8, 1), p=disabled_dock_prob)
            disabled_docks = np.minimum(disabled_docks, capacities - bikes)
            num_docks_available = capacities - bikes - disabled_docks
            num_ebikes = np.minimum(
                self._rng.binomial(n=bikes, p=ebike_fraction),
                bikes,
            )
            num_bikes_disabled = self._rng.binomial(n=bikes, p=0.02)

            for s_idx, station_id in enumerate(station_ids):
                telemetry_records.append({
                    "timestamp": ts,
                    "region_id": df_stations.iloc[s_idx]["region_id"],
                    "lat": float(df_stations.iloc[s_idx]["lat"]),
                    "lon": float(df_stations.iloc[s_idx]["lon"]),
                    "station_id": station_id,
                    "capacity": int(capacities[s_idx]),
                    "short_name": df_stations.iloc[s_idx]["short_name"],
                    "name": df_stations.iloc[s_idx]["name"],
                    "num_docks_available": int(num_docks_available[s_idx]),
                    "is_returning": int(df_stations.iloc[s_idx]["is_returning"]),
                    "last_reported": ts,
                    "num_bikes_available": int(bikes[s_idx]),
                    "is_installed": int(df_stations.iloc[s_idx]["is_installed"]),
                    "is_renting": int(df_stations.iloc[s_idx]["is_renting"]),
                    "num_ebikes_available": int(num_ebikes[s_idx]),
                    "num_docks_disabled": int(disabled_docks[s_idx]),
                    "num_bikes_disabled": int(num_bikes_disabled[s_idx]),
                })

        df_telemetry_ts = pd.DataFrame(telemetry_records)
        df_trips = pd.DataFrame(trip_records)
        if df_trips.empty:
            df_trips = pd.DataFrame(columns=[
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
            ])
        else:
            df_trips = df_trips.sort_values("started_at").reset_index(drop=True)

        return df_inventory_ts, df_telemetry_ts, df_trips

    def _generate_costs(
        self,
        df_stations: pd.DataFrame,
        df_resources: pd.DataFrame,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        station_costs = pd.DataFrame({
            "station_id": df_stations["node_id"],
            "fixed_cost_per_visit": np.round(self._rng.uniform(50.0, 150.0, size=len(df_stations)), 2),
            "cost_per_bike_moved": np.round(self._rng.uniform(2.0, 5.0, size=len(df_stations)), 2),
        })

        truck_rates = pd.DataFrame({
            "resource_id": df_resources["resource_id"],
            "cost_per_km": np.round(self._rng.uniform(1.5, 3.0, size=len(df_resources)), 2),
            "cost_per_hour": np.round(self._rng.uniform(25.0, 45.0, size=len(df_resources)), 2),
            "fixed_dispatch_cost": np.round(self._rng.uniform(100.0, 200.0, size=len(df_resources)), 2),
        })

        return station_costs, truck_rates
