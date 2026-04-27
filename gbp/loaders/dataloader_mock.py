from __future__ import annotations

import uuid

import numpy as np
import pandas as pd

COMMODITY_CATEGORIES = ("electric_bike", "classic_bike")


class DataLoaderMock:
    """Generate Citi Bike-like temporal mock data for rebalancing tests.

    Required by ``BikeShareSourceProtocol``:
        - df_stations             [station_id, lat, lon]
        - df_depots               [node_id, lat, lon]
        - df_resources            [resource_id]
        - df_station_capacities   [station_id, commodity_category, capacity]
        - df_depot_capacities     [node_id, commodity_category, capacity]
        - df_resource_capacities  [resource_id, capacity]
        - timestamps              DatetimeIndex
        - inventory_initial       [facility_id, commodity_category, quantity]
        - df_telemetry_ts         station telemetry (GBFS-like)
        - df_trips                trip records with rideable_type as commodity
        - df_station_costs        [station_id, fixed_cost_station]
        - df_depot_costs          [node_id, fixed_cost_depot]
        - df_truck_rates          [resource_id, cost_per_km, cost_per_hour, fixed_dispatch_cost]
    """

    _GROUPS: dict[str, list[str]] = {
        "stations": ["df_stations", "df_station_capacities", "df_station_costs"],
        "depots": ["df_depots", "df_depot_capacities", "df_depot_costs"],
        "resources": ["df_resources", "df_resource_capacities", "df_truck_rates"],
        "observations": ["inventory_initial", "df_telemetry_ts", "df_trips"],
    }

    def __init__(self, config: dict):
        self.config = config
        self._rng = np.random.default_rng(seed=self.config.get("seed", 42))

    def load_data(self) -> None:
        n_stations = self.config["n_stations"]
        n_depots = self.config.get("n_depots", 2)
        n_timestamps = self.config.get("n_timestamps", 168)
        start_date = self.config.get("start_date", "2025-01-01")
        freq = self.config.get("time_freq", "h")
        ebike_fraction = float(self.config.get("ebike_fraction", 0.3))
        depot_capacity = self.config.get("depot_capacity", 200)

        # Stations — public table is minimal, full is internal
        full_stations = self._generate_stations_full(n_stations)
        self.df_stations = full_stations[["station_id", "lat", "lon"]].copy()

        # Station capacities per commodity_category
        total_caps = full_stations["inventory_capacity"].values
        ebike_caps = np.maximum(1, np.round(total_caps * ebike_fraction).astype(int))
        classic_caps = total_caps - ebike_caps
        cap_rows: list[dict] = []
        for i, sid in enumerate(full_stations["station_id"]):
            cap_rows.append({
                "station_id": sid,
                "commodity_category": "electric_bike",
                "capacity": int(ebike_caps[i]),
            })
            cap_rows.append({
                "station_id": sid,
                "commodity_category": "classic_bike",
                "capacity": int(classic_caps[i]),
            })
        self.df_station_capacities = pd.DataFrame(cap_rows)

        # Depots
        self.df_depots = self._generate_depots(n_depots)

        # Depot capacities per commodity_category
        depot_ebike_cap = max(1, round(depot_capacity * ebike_fraction))
        depot_classic_cap = depot_capacity - depot_ebike_cap
        depot_cap_rows: list[dict] = []
        for nid in self.df_depots["node_id"]:
            depot_cap_rows.append({
                "node_id": nid,
                "commodity_category": "electric_bike",
                "capacity": depot_ebike_cap,
            })
            depot_cap_rows.append({
                "node_id": nid,
                "commodity_category": "classic_bike",
                "capacity": depot_classic_cap,
            })
        self.df_depot_capacities = pd.DataFrame(depot_cap_rows)

        # Resources — no capacity on the table
        num_resources = self.config.get("num_resources", 3)
        resource_cap = self.config.get("resource_capacity", 100)
        resource_ids = [f"truck_{i + 1}" for i in range(num_resources)]
        self.df_resources = pd.DataFrame({"resource_id": resource_ids})
        self.df_resource_capacities = pd.DataFrame({
            "resource_id": resource_ids,
            "capacity": [resource_cap] * num_resources,
        })

        self.timestamps = pd.date_range(start=start_date, periods=n_timestamps, freq=freq)

        self.inventory_initial, self.df_telemetry_ts, self.df_trips = (
            self._generate_initial_telemetry_trips(
                df_stations=full_stations,
                df_depots=self.df_depots,
                timestamps=self.timestamps,
                ebike_fraction=ebike_fraction,
                depot_ebike_cap=depot_ebike_cap,
                depot_classic_cap=depot_classic_cap,
            )
        )

        self.df_station_costs, self.df_depot_costs, self.df_truck_rates = self._generate_costs(
            df_stations=self.df_stations,
            df_depots=self.df_depots,
            df_resource_capacities=self.df_resource_capacities,
        )

    # ── display ───────────────────────────────────────────────────────

    def _is_loaded(self) -> bool:
        return getattr(self, "df_stations", None) is not None

    @staticmethod
    def _format_columns(df: pd.DataFrame, max_cols: int = 6) -> str:
        if isinstance(df.columns, pd.MultiIndex):
            cols = [str(tuple(c)) for c in df.columns[:max_cols]]
        else:
            cols = [str(c) for c in df.columns[:max_cols]]
        out = ", ".join(cols)
        if len(df.columns) > max_cols:
            out += f", ... (+{len(df.columns) - max_cols})"
        return out

    def __repr__(self) -> str:
        """Compact one-line-per-group summary of generated DataFrames."""
        cls_name = type(self).__name__
        if not self._is_loaded():
            return f"{cls_name}(not loaded — call load_data())"

        table_count = 0
        total_rows = 0
        detail_parts: list[str] = []
        for group_name, field_names in self._GROUPS.items():
            items: list[str] = []
            for fname in field_names:
                df = getattr(self, fname, None)
                if isinstance(df, pd.DataFrame):
                    table_count += 1
                    total_rows += len(df)
                    items.append(f"{fname}={len(df)}")
            if items:
                detail_parts.append(f"  {group_name}: {', '.join(items)}")

        ts = getattr(self, "timestamps", None)
        if ts is not None and len(ts):
            detail_parts.append(
                f"  temporal: timestamps={len(ts)} "
                f"({ts[0]}..{ts[-1]}, freq={ts.freq})"
            )

        header = f"{cls_name}(tables={table_count}, total_rows={total_rows})"
        return header + "\n" + "\n".join(detail_parts) if detail_parts else header

    def _repr_html_(self) -> str:
        cls_name = type(self).__name__
        if not self._is_loaded():
            return (
                f"<div><strong>{cls_name}</strong> "
                f"<em>not loaded — call load_data()</em></div>"
            )

        rows_html: list[str] = []
        for group_name, field_names in self._GROUPS.items():
            present = [
                f for f in field_names
                if isinstance(getattr(self, f, None), pd.DataFrame)
            ]
            first_in_group = True
            for fname in present:
                df = getattr(self, fname)
                cols = self._format_columns(df)
                group_cell = (
                    f'<td rowspan="{len(present)}" '
                    f'style="vertical-align:top;font-weight:bold;background:#f0f0f0">'
                    f"{group_name}</td>"
                    if first_in_group else ""
                )
                rows_html.append(
                    f"<tr>{group_cell}"
                    f"<td>{fname}</td>"
                    f"<td style='text-align:right'>{len(df)}</td>"
                    f"<td><code style='font-size:0.85em'>{cols}</code></td>"
                    f"</tr>"
                )
                first_in_group = False

        ts = getattr(self, "timestamps", None)
        if ts is not None and len(ts):
            rows_html.append(
                f"<tr><td style='font-weight:bold;background:#f0f0f0'>temporal</td>"
                f"<td>timestamps</td>"
                f"<td style='text-align:right'>{len(ts)}</td>"
                f"<td><code style='font-size:0.85em'>"
                f"{ts[0]}..{ts[-1]}, freq={ts.freq}"
                f"</code></td></tr>"
            )

        total_tables = sum(
            1 for g in self._GROUPS.values() for f in g
            if isinstance(getattr(self, f, None), pd.DataFrame)
        )

        return (
            f"<div><strong>{cls_name}</strong> "
            f"<span style='color:#666'>({total_tables} tables)</span>"
            f"<table style='margin-top:4px;border-collapse:collapse;font-size:0.9em'>"
            f"<tr style='border-bottom:1px solid #ccc'>"
            f"<th>Group</th><th>Table</th><th>Rows</th><th>Columns</th></tr>"
            + "\n".join(rows_html)
            + "</table></div>"
        )

    # ------------------------------------------------------------------

    def _generate_stations_full(self, n: int) -> pd.DataFrame:
        """Generate full station data (internal); public df_stations is slimmed."""
        station_ids = [f"station_{i + 1}" for i in range(n)]
        street_prefix = np.array(["W", "E"])
        avenues = np.array([
            "1 Ave", "2 Ave", "3 Ave", "5 Ave", "6 Ave", "7 Ave", "Broadway", "Park Ave",
        ])

        street_num_1 = self._rng.integers(10, 95, size=n)
        street_num_2 = self._rng.integers(10, 95, size=n)
        dirs_1 = self._rng.choice(street_prefix, size=n)
        dirs_2 = self._rng.choice(street_prefix, size=n)
        ave = self._rng.choice(avenues, size=n)

        names = [
            f"{d1} {s1} St & {a}"
            for d1, s1, a in zip(dirs_1, street_num_1, ave, strict=False)
        ]
        short_names = [f"{d2} {s2}" for d2, s2 in zip(dirs_2, street_num_2, strict=False)]

        return pd.DataFrame({
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
        })

    def _generate_depots(self, n: int) -> pd.DataFrame:
        return pd.DataFrame({
            "node_id": [f"depot_{i + 1}" for i in range(n)],
            "lat": self._rng.uniform(40.68, 40.86, size=n),
            "lon": self._rng.uniform(-74.03, -73.90, size=n),
        })

    def _generate_initial_telemetry_trips(
        self,
        df_stations: pd.DataFrame,
        df_depots: pd.DataFrame,
        timestamps: pd.DatetimeIndex,
        ebike_fraction: float,
        depot_ebike_cap: int,
        depot_classic_cap: int,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Pick a random initial inventory, then generate trips constrained by it.

        Returns ``(inventory_initial, df_telemetry_ts, df_trips)``.  The full
        hourly inventory matrix is kept as a local numpy array — it is only
        used to drive the constrained trip generation and to seed the
        GBFS-like telemetry — and is never exposed publicly.

        Contract with the downstream simulator (Demand + Arrivals only):

        1. Every trip is strictly intra-day: both ``started_at`` and ``ended_at``
           fall within the same calendar day.
        2. No trip event lands in the *first* hour of any day.  This keeps
           the hour-zero inventory equal to the "true start of day" state
           (nothing applied yet) — ``inventory_initial`` is exactly that
           state, which the simulator consumes at init time.
        3. Trip deltas are applied via cumulative broadcast on the local
           numpy matrix (``inv[start_hour:, start_idx] -= 1`` and
           ``inv[end_hour:, end_idx] += 1``).  Combined with (1) and (2)
           this guarantees, for every day ``D``::

               inv[last_hour_of_D] ==
                   inv[first_hour_of_D]
                   + arrivals_on_D(by ended_at date)
                   - departures_on_D(by started_at date)

        4. Capacity is enforced per commodity per station at the exact hour
           of departure/arrival, so the hourly trajectory stays in
           ``[0, per_commodity_capacity]``.

        Depot inventory is constant in the mock — set to its per-commodity
        capacity for both ``inventory_initial`` and the telemetry seed.
        """
        total_capacities = df_stations["inventory_capacity"].to_numpy(dtype=int)
        station_ids = df_stations["station_id"].to_numpy()
        depot_ids = df_depots["node_id"].to_numpy()
        n_stations = len(station_ids)
        n_steps = len(timestamps)

        # Per-commodity station capacities (match df_station_capacities split).
        ebike_caps = np.maximum(1, np.round(total_capacities * ebike_fraction).astype(int))
        classic_caps = total_capacities - ebike_caps

        member_fraction = float(self.config.get("member_fraction", 0.7))
        avg_trip_duration_min = int(self.config.get("avg_trip_duration_min", 15))
        disabled_dock_prob = float(self.config.get("disabled_dock_prob", 0.05))
        trips_per_hour_per_station = float(self.config.get("trips_per_hour_per_station", 0.7))
        min_duration = max(5, avg_trip_duration_min - 10)
        max_duration = max(min_duration + 1, avg_trip_duration_min + 15)

        # ── Initial inventory (clean, no events applied yet) ─────────
        init_electric = self._rng.integers(low=0, high=ebike_caps + 1)
        init_classic = self._rng.integers(low=0, high=classic_caps + 1)
        inv_electric = np.tile(init_electric, (n_steps, 1))
        inv_classic = np.tile(init_classic, (n_steps, 1))

        # ── Group timestamp indices by calendar day ─────────────────
        ts_dates = pd.DatetimeIndex(timestamps).normalize()
        unique_dates = pd.Index(ts_dates).unique()
        day_bounds: list[tuple[pd.Timestamp, int, int]] = []
        for d in unique_dates:
            hours_in_day = np.where(ts_dates == d)[0]
            if len(hours_in_day) == 0:
                continue
            day_bounds.append((d, int(hours_in_day[0]), int(hours_in_day[-1])))

        trip_records: list[dict] = []

        # ── Generate trips day by day (chronological within each day) ─
        for _date, first_h, last_h in day_bounds:
            day_length_hours = last_h - first_h + 1
            # Need at least 2 hours so that first_h is kept event-free
            # and there is still room for a start + arrival within the day.
            if day_length_hours < 2:
                continue

            # Start offset is in minutes from timestamps[first_h].
            # Allowed window: start_off in [60, day_length_hours*60 - max_duration)
            # which enforces start_hour >= first_h + 1 and end_hour <= last_h.
            earliest_start_off = 60
            latest_start_off = day_length_hours * 60 - max_duration
            if latest_start_off <= earliest_start_off:
                continue

            est_trips = n_stations * trips_per_hour_per_station * day_length_hours
            n_trip_events = int(self._rng.poisson(lam=max(est_trips, 1.0)))
            if n_trip_events == 0:
                continue

            start_offsets = self._rng.integers(
                earliest_start_off, latest_start_off, size=n_trip_events,
            )
            durations = self._rng.integers(min_duration, max_duration, size=n_trip_events)
            is_electric_arr = self._rng.random(n_trip_events) < ebike_fraction

            # Process chronologically so later trips see the updated state.
            order = np.argsort(start_offsets)
            day_start_ts = pd.Timestamp(timestamps[first_h])

            for idx in order:
                start_off = int(start_offsets[idx])
                dur = int(durations[idx])
                end_off = start_off + dur

                start_hour = first_h + start_off // 60
                end_hour = first_h + end_off // 60
                if end_hour > last_h:
                    continue  # safety — bounds above should prevent this

                is_electric = bool(is_electric_arr[idx])
                source_inv = inv_electric if is_electric else inv_classic
                source_cap = ebike_caps if is_electric else classic_caps
                rideable_type = "electric_bike" if is_electric else "classic_bike"

                candidate_starts = np.where(source_inv[start_hour] > 0)[0]
                if len(candidate_starts) == 0:
                    continue
                start_idx = int(self._rng.choice(candidate_starts))

                room_mask = source_inv[end_hour] + 1 <= source_cap
                not_self = np.arange(n_stations) != start_idx
                candidate_ends = np.where(room_mask & not_self)[0]
                if len(candidate_ends) == 0:
                    continue
                end_idx = int(self._rng.choice(candidate_ends))

                # Broadcast deltas: bike leaves start at start_hour and arrives
                # at end at end_hour. Both are within [first_h + 1, last_h].
                source_inv[start_hour:, start_idx] -= 1
                source_inv[end_hour:, end_idx] += 1

                started_at = day_start_ts + pd.Timedelta(minutes=start_off)
                ended_at = day_start_ts + pd.Timedelta(minutes=end_off)
                member_casual = (
                    "member" if self._rng.random() < member_fraction else "casual"
                )
                trip_records.append({
                    "ride_id": str(uuid.uuid4()),
                    "rideable_type": rideable_type,
                    "started_at": started_at,
                    "ended_at": ended_at,
                    "start_station_name": df_stations.iloc[start_idx]["name"],
                    "start_station_id": df_stations.iloc[start_idx]["station_id"],
                    "end_station_name": df_stations.iloc[end_idx]["name"],
                    "end_station_id": df_stations.iloc[end_idx]["station_id"],
                    "start_lat": float(df_stations.iloc[start_idx]["lat"]),
                    "start_lng": float(df_stations.iloc[start_idx]["lon"]),
                    "end_lat": float(df_stations.iloc[end_idx]["lat"]),
                    "end_lng": float(df_stations.iloc[end_idx]["lon"]),
                    "member_casual": member_casual,
                })

        # ── Build inventory_initial (long format: one row per facility×commodity) ─
        inv_initial_rows: list[dict] = []
        for i, sid in enumerate(station_ids):
            inv_initial_rows.append({
                "facility_id": sid,
                "commodity_category": "electric_bike",
                "quantity": int(init_electric[i]),
            })
            inv_initial_rows.append({
                "facility_id": sid,
                "commodity_category": "classic_bike",
                "quantity": int(init_classic[i]),
            })
        for did in depot_ids:
            inv_initial_rows.append({
                "facility_id": did,
                "commodity_category": "electric_bike",
                "quantity": depot_ebike_cap,
            })
            inv_initial_rows.append({
                "facility_id": did,
                "commodity_category": "classic_bike",
                "quantity": depot_classic_cap,
            })
        df_inventory_initial = pd.DataFrame(inv_initial_rows)

        # ── Telemetry (station-only, GBFS-like) ─────────────────────
        telemetry_records: list[dict] = []
        for t_idx, ts in enumerate(timestamps):
            bikes_e = inv_electric[t_idx]
            bikes_c = inv_classic[t_idx]
            bikes_total = bikes_e + bikes_c
            disabled_docks = self._rng.binomial(
                n=np.maximum(total_capacities // 8, 1), p=disabled_dock_prob,
            )
            disabled_docks = np.minimum(disabled_docks, total_capacities - bikes_total)
            num_docks_available = total_capacities - bikes_total - disabled_docks
            num_bikes_disabled = self._rng.binomial(n=bikes_total, p=0.02)

            for s_idx, station_id in enumerate(station_ids):
                telemetry_records.append({
                    "timestamp": ts,
                    "region_id": df_stations.iloc[s_idx]["region_id"],
                    "lat": float(df_stations.iloc[s_idx]["lat"]),
                    "lon": float(df_stations.iloc[s_idx]["lon"]),
                    "station_id": station_id,
                    "capacity": int(total_capacities[s_idx]),
                    "short_name": df_stations.iloc[s_idx]["short_name"],
                    "name": df_stations.iloc[s_idx]["name"],
                    "num_docks_available": int(num_docks_available[s_idx]),
                    "is_returning": int(df_stations.iloc[s_idx]["is_returning"]),
                    "last_reported": ts,
                    "num_bikes_available": int(bikes_total[s_idx]),
                    "is_installed": int(df_stations.iloc[s_idx]["is_installed"]),
                    "is_renting": int(df_stations.iloc[s_idx]["is_renting"]),
                    "num_ebikes_available": int(bikes_e[s_idx]),
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

        return df_inventory_initial, df_telemetry_ts, df_trips

    def _generate_costs(
        self,
        df_stations: pd.DataFrame,
        df_depots: pd.DataFrame,
        df_resource_capacities: pd.DataFrame,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        station_costs = pd.DataFrame({
            "station_id": df_stations["station_id"],
            "fixed_cost_station": np.round(
                self._rng.uniform(50.0, 150.0, size=len(df_stations)), 2,
            ),
        })

        depot_costs = pd.DataFrame({
            "node_id": df_depots["node_id"],
            "fixed_cost_depot": np.round(
                self._rng.uniform(80.0, 200.0, size=len(df_depots)), 2,
            ),
        })

        truck_rates = pd.DataFrame({
            "resource_id": df_resource_capacities["resource_id"],
            "cost_per_km": np.round(
                self._rng.uniform(1.5, 3.0, size=len(df_resource_capacities)), 2,
            ),
            "cost_per_hour": np.round(
                self._rng.uniform(25.0, 45.0, size=len(df_resource_capacities)), 2,
            ),
            "fixed_dispatch_cost": np.round(
                self._rng.uniform(100.0, 200.0, size=len(df_resource_capacities)), 2,
            ),
        })

        return station_costs, depot_costs, truck_rates
