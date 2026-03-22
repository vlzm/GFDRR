"""Graph loader: mock/CSV sources → ``RawModelData`` → ``build_model`` → ``ResolvedModelData``.

Static network and cost tables are derived once in ``load_data()``.  Hourly inventory
stays on the source as ``df_inventory_ts``; use ``inventory_timeseries`` or
``rebalancer_snapshot(ts)`` for time-sliced views.

Usage::

    from gbp.loaders import DataLoaderMock, DataLoaderGraph, GraphLoaderConfig

    mock = DataLoaderMock({"n": 10})
    loader = DataLoaderGraph(mock, GraphLoaderConfig())
    loader.load_data()

    resolved = loader.resolved
    snap = loader.rebalancer_snapshot(pd.Timestamp("2025-01-03 12:00"))
"""

from __future__ import annotations

import math
from dataclasses import dataclass
import pandas as pd
import structlog

from gbp.build.pipeline import build_model
from gbp.core.enums import FacilityRole, ModalType, PeriodType
from gbp.core.model import RawModelData, ResolvedModelData

from .contracts import (
    DepotsSourceSchema,
    GraphLoaderConfig,
    ResourcesSourceSchema,
    StationsSourceSchema,
)
from .protocols import DataSourceProtocol

log = structlog.get_logger()

COMMODITY_CATEGORY = "working_bike"
RESOURCE_CATEGORY = "rebalancing_truck"


@dataclass
class SnapshotAttributeTable:
    """Subset of legacy ``AttributeTable`` fields used by ``DataLoaderRebalancer``."""

    name: str
    entity_type: str
    attribute_class: str
    granularity_keys: list[str]
    value_columns: list[str]
    value_types: dict[str, str]
    data: pd.DataFrame


@dataclass
class RebalancerGraphSnapshot:
    """Minimal graph-shaped view for ``DataLoaderRebalancer`` (legacy PDP pipeline)."""

    nodes: pd.DataFrame
    coordinates: pd.DataFrame
    resources: pd.DataFrame
    node_attributes: dict[str, SnapshotAttributeTable]
    inventory: pd.DataFrame
    distance_service: EdgeDistanceLookup | None


class EdgeDistanceLookup:
    """Directed road distance (km) from resolved edges, with coordinate fallback."""

    def __init__(
        self,
        edges: pd.DataFrame | None,
        latlon_by_id: dict[str, tuple[float, float]],
        *,
        backend: str,
    ) -> None:
        self._backend = backend
        self._latlon = latlon_by_id
        self._km: dict[tuple[str, str], float] = {}
        if edges is not None and not edges.empty and "distance" in edges.columns:
            sub = edges[edges.get("modal_type", ModalType.ROAD.value) == ModalType.ROAD.value]
            for _, row in sub.iterrows():
                a, b = str(row["source_id"]), str(row["target_id"])
                self._km[(a, b)] = float(row["distance"])

    def get_distance(self, source_id: str, target_id: str) -> float:
        key = (source_id, target_id)
        if key in self._km:
            return self._km[key]
        p0 = self._latlon.get(source_id)
        p1 = self._latlon.get(target_id)
        if p0 is None or p1 is None:
            return 0.0
        return _pair_distance_km(p0[0], p0[1], p1[0], p1[1], self._backend)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    rlat1, rlon1 = math.radians(lat1), math.radians(lon1)
    rlat2, rlon2 = math.radians(lat2), math.radians(lon2)
    dlat, dlon = rlat2 - rlat1, rlon2 - rlon1
    h = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return 2 * 6_371.0 * math.asin(min(1.0, math.sqrt(h)))


def _euclidean_latlon_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    km_per_deg_lat = 111.0
    km_per_deg_lon = 111.0 * math.cos(math.radians((lat1 + lat2) / 2))
    dx = (lon2 - lon1) * km_per_deg_lon
    dy = (lat2 - lat1) * km_per_deg_lat
    return math.sqrt(dx * dx + dy * dy)


def _pair_distance_km(
    lat1: float, lon1: float, lat2: float, lon2: float, backend: str,
) -> float:
    if backend == "euclidean":
        return _euclidean_latlon_km(lat1, lon1, lat2, lon2)
    return _haversine_km(lat1, lon1, lat2, lon2)


class DataLoaderGraph:
    """Build ``RawModelData`` from a ``DataSourceProtocol`` and run ``gbp.build.build_model``."""

    def __init__(
        self,
        source: DataSourceProtocol,
        config: GraphLoaderConfig | None = None,
    ) -> None:
        self._source = source
        self._config = config or GraphLoaderConfig()
        self._log = log.bind(loader="graph_core")
        self._raw: RawModelData | None = None
        self._resolved: ResolvedModelData | None = None
        self._inventory_ts: pd.DataFrame | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_data(self) -> None:
        """Load source tables, assemble ``RawModelData``, run the build pipeline."""
        self._log.info("load_start")
        self._source.load_data()
        self._validate_source()

        self._raw = self._build_raw_model()
        self._raw.validate()
        self._resolved = build_model(self._raw)
        self._inventory_ts = self._source.df_inventory_ts

        n_fac = len(self._resolved.facilities)
        n_e = len(self._resolved.edges) if self._resolved.edges is not None else 0
        self._log.info("load_done", facilities=n_fac, edges=n_e)

    @property
    def raw(self) -> RawModelData:
        if self._raw is None:
            raise ValueError("Data is not loaded. Call load_data() first.")
        return self._raw

    @property
    def resolved(self) -> ResolvedModelData:
        if self._resolved is None:
            raise ValueError("Data is not loaded. Call load_data() first.")
        return self._resolved

    @property
    def available_dates(self) -> pd.DatetimeIndex:
        return self._source.timestamps

    @property
    def inventory_timeseries(self) -> pd.DataFrame:
        if self._inventory_ts is None:
            return pd.DataFrame()
        return self._inventory_ts

    @property
    def inventory_ts(self) -> pd.DataFrame:
        """Alias for tests and notebooks that mutate hourly inventory in place."""
        if self._inventory_ts is None:
            raise ValueError("Data is not loaded. Call load_data() first.")
        return self._inventory_ts

    @property
    def telemetry_ts(self) -> pd.DataFrame:
        return self._source.df_telemetry_ts

    @property
    def trip_flows_hourly(self) -> pd.DataFrame:
        """Aggregated hourly trip counts (source station → target station), from ``df_trips``."""
        return _trip_flows_hourly(self._source.df_trips)

    def rebalancer_snapshot(self, date: pd.Timestamp) -> RebalancerGraphSnapshot:
        """Thin view for ``DataLoaderRebalancer`` (nodes, coords, inventory, distances)."""
        res = self.resolved
        src = self._source
        inv_ts = self.inventory_ts
        if date not in inv_ts.index:
            nearest = inv_ts.index[inv_ts.index.get_indexer([date], method="nearest")[0]]
            date = pd.Timestamp(nearest)

        stations = src.df_stations
        depots = src.df_depots
        station_ids = list(stations["node_id"].astype(str))
        depot_ids = list(depots["node_id"].astype(str))

        nodes = pd.concat(
            [
                pd.DataFrame({"id": station_ids, "node_type": "station"}),
                pd.DataFrame({"id": depot_ids, "node_type": "depot"}),
            ],
            ignore_index=True,
        )

        coord_rows = []
        for _, r in stations.iterrows():
            coord_rows.append(
                {
                    "node_id": str(r["node_id"]),
                    "latitude": float(r["lat"]),
                    "longitude": float(r["lon"]),
                },
            )
        for _, r in depots.iterrows():
            coord_rows.append(
                {
                    "node_id": str(r["node_id"]),
                    "latitude": float(r["lat"]),
                    "longitude": float(r["lon"]),
                },
            )
        coordinates = pd.DataFrame(coord_rows)

        latlon = {row["node_id"]: (row["latitude"], row["longitude"]) for _, row in coordinates.iterrows()}

        resources = pd.DataFrame(
            {
                "id": src.df_resources["resource_id"].values,
                "resource_type": "vehicle",
                "capacity": src.df_resources["capacity"].values,
            },
        )
        tr = src.df_truck_rates
        if tr is not None and not tr.empty:
            resources = resources.merge(tr, left_on="id", right_on="resource_id", how="left").drop(
                columns=["resource_id"], errors="ignore",
            )

        cap_data = stations[["node_id", "inventory_capacity"]].rename(
            columns={"inventory_capacity": "value"},
        )
        node_attributes: dict[str, SnapshotAttributeTable] = {
            "inventory_capacity": SnapshotAttributeTable(
                name="inventory_capacity",
                entity_type="node",
                attribute_class="capacity",
                granularity_keys=["node_id"],
                value_columns=["value"],
                value_types={"value": "int"},
                data=cap_data,
            ),
        }

        costs = src.df_station_costs
        if costs is not None and not costs.empty:
            fc = costs.rename(columns={"station_id": "node_id"})[["node_id", "fixed_cost_per_visit"]].rename(
                columns={"fixed_cost_per_visit": "value"},
            )
            vc = costs.rename(columns={"station_id": "node_id"})[["node_id", "cost_per_bike_moved"]].rename(
                columns={"cost_per_bike_moved": "value"},
            )
            node_attributes["station_fixed_cost"] = SnapshotAttributeTable(
                name="station_fixed_cost",
                entity_type="node",
                attribute_class="cost",
                granularity_keys=["node_id"],
                value_columns=["value"],
                value_types={"value": "float"},
                data=fc,
            )
            node_attributes["station_variable_cost"] = SnapshotAttributeTable(
                name="station_variable_cost",
                entity_type="node",
                attribute_class="cost",
                granularity_keys=["node_id"],
                value_columns=["value"],
                value_types={"value": "float"},
                data=vc,
            )

        if {"node_id", "name", "short_name"}.issubset(stations.columns):
            node_attributes["station_info"] = SnapshotAttributeTable(
                name="station_info",
                entity_type="node",
                attribute_class="property",
                granularity_keys=["node_id"],
                value_columns=["name", "short_name"],
                value_types={"name": "str", "short_name": "str"},
                data=stations[["node_id", "name", "short_name"]].copy(),
            )

        row = inv_ts.loc[date]
        inventory = pd.DataFrame(
            {
                "node_id": station_ids,
                "commodity_id": ["bike"] * len(station_ids),
                "quantity": [int(row[sid]) for sid in station_ids],
            },
        )

        edges_for_dist = (
            res.edges
            if self._config.build_edges and res.edges is not None and not res.edges.empty
            else None
        )
        dist = EdgeDistanceLookup(edges_for_dist, latlon, backend=self._config.distance_backend)

        return RebalancerGraphSnapshot(
            nodes=nodes,
            coordinates=coordinates,
            resources=resources,
            node_attributes=node_attributes,
            inventory=inventory,
            distance_service=dist,
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _validate_source(self) -> None:
        StationsSourceSchema.validate(self._source.df_stations)
        DepotsSourceSchema.validate(self._source.df_depots)
        ResourcesSourceSchema.validate(self._source.df_resources)
        self._log.debug("source_validated")

    def _build_raw_model(self) -> RawModelData:
        ts = self._source.timestamps
        start_d = pd.Timestamp(ts[0]).normalize().date()
        end_d = (pd.Timestamp(ts[-1]).normalize() + pd.Timedelta(days=1)).date()

        unique_days = pd.date_range(
            start=pd.Timestamp(ts[0]).normalize(),
            end=pd.Timestamp(ts[-1]).normalize(),
            freq="D",
        )
        period_rows = []
        for i, day in enumerate(unique_days):
            d0 = day.date()
            d1 = (day + pd.Timedelta(days=1)).date()
            period_rows.append(
                {
                    "period_id": f"p{i}",
                    "planning_horizon_id": "h1",
                    "segment_index": 0,
                    "period_index": i,
                    "period_type": PeriodType.DAY.value,
                    "start_date": d0,
                    "end_date": d1,
                },
            )
        periods = pd.DataFrame(period_rows)

        planning_horizon = pd.DataFrame(
            {
                "planning_horizon_id": ["h1"],
                "name": ["mock_horizon"],
                "start_date": [start_d],
                "end_date": [end_d],
            },
        )
        planning_horizon_segments = pd.DataFrame(
            {
                "planning_horizon_id": ["h1"],
                "segment_index": [0],
                "start_date": [start_d],
                "end_date": [end_d],
                "period_type": PeriodType.DAY.value,
            },
        )

        stations = self._source.df_stations
        depots = self._source.df_depots

        fac_stations = pd.DataFrame(
            {
                "facility_id": stations["node_id"].astype(str),
                "facility_type": "station",
                "name": stations["name"] if "name" in stations.columns else stations["node_id"].astype(str),
                "lat": stations["lat"].astype(float),
                "lon": stations["lon"].astype(float),
            },
        )
        fac_depots = pd.DataFrame(
            {
                "facility_id": depots["node_id"].astype(str),
                "facility_type": "depot",
                "name": depots["node_id"].astype(str),
                "lat": depots["lat"].astype(float),
                "lon": depots["lon"].astype(float),
            },
        )
        facilities = pd.concat([fac_depots, fac_stations], ignore_index=True)

        station_ids = list(fac_stations["facility_id"])
        depot_ids = list(fac_depots["facility_id"])

        role_rows: list[dict] = []
        op_rows: list[dict] = []
        for did in depot_ids:
            role_rows.extend(
                [
                    {"facility_id": did, "role": FacilityRole.STORAGE.value},
                    {"facility_id": did, "role": FacilityRole.TRANSSHIPMENT.value},
                ],
            )
            op_rows.extend(
                [
                    {"facility_id": did, "operation_type": "receiving", "enabled": True},
                    {"facility_id": did, "operation_type": "storage", "enabled": True},
                    {"facility_id": did, "operation_type": "dispatch", "enabled": True},
                ],
            )
        for sid in station_ids:
            role_rows.extend(
                [
                    {"facility_id": sid, "role": FacilityRole.SINK.value},
                    {"facility_id": sid, "role": FacilityRole.STORAGE.value},
                    {"facility_id": sid, "role": FacilityRole.SOURCE.value},
                ],
            )
            op_rows.extend(
                [
                    {"facility_id": sid, "operation_type": "receiving", "enabled": True},
                    {"facility_id": sid, "operation_type": "storage", "enabled": True},
                    {"facility_id": sid, "operation_type": "dispatch", "enabled": True},
                ],
            )
        facility_roles = pd.DataFrame(role_rows)
        facility_operations = pd.DataFrame(op_rows)

        if self._config.build_edges:
            edge_rules = pd.DataFrame(
                {
                    "source_type": ["depot", "station", "station", "depot"],
                    "target_type": ["station", "depot", "station", "depot"],
                    "commodity_category": [COMMODITY_CATEGORY] * 4,
                    "modal_type": [ModalType.ROAD.value] * 4,
                    "enabled": [True] * 4,
                },
            )
        else:
            edge_rules = pd.DataFrame(
                {
                    "source_type": pd.Series(dtype="string"),
                    "target_type": pd.Series(dtype="string"),
                    "commodity_category": pd.Series(dtype="string"),
                    "modal_type": pd.Series(dtype="string"),
                    "enabled": pd.Series(dtype="bool"),
                },
            )

        latlon = {
            str(r["facility_id"]): (float(r["lat"]), float(r["lon"]))
            for _, r in facilities.iterrows()
        }
        ids = list(facilities["facility_id"].astype(str))

        edges_df: pd.DataFrame | None = None
        ec_df: pd.DataFrame | None = None
        if self._config.build_edges:
            edge_records: list[dict] = []
            ec_records: list[dict] = []
            speed = self._config.default_speed_kmh
            for i, a in enumerate(ids):
                la0, lo0 = latlon[a]
                for j, b in enumerate(ids):
                    if i == j:
                        continue
                    la1, lo1 = latlon[b]
                    dkm = _pair_distance_km(la0, lo0, la1, lo1, self._config.distance_backend)
                    lt_h = dkm / speed if speed > 0 else 0.0
                    edge_records.append(
                        {
                            "source_id": a,
                            "target_id": b,
                            "modal_type": ModalType.ROAD.value,
                            "distance": dkm,
                            "distance_unit": "km",
                            "lead_time_hours": max(lt_h, 1e-6),
                            "reliability": None,
                        },
                    )
                    ec_records.append(
                        {
                            "source_id": a,
                            "target_id": b,
                            "modal_type": ModalType.ROAD.value,
                            "commodity_category": COMMODITY_CATEGORY,
                            "enabled": True,
                            "capacity_consumption": 1.0,
                        },
                    )
            edges_df = pd.DataFrame(edge_records)
            ec_df = pd.DataFrame(ec_records)

        inv0 = self._source.df_inventory_ts.iloc[0]
        inventory_initial = pd.DataFrame(
            {
                "facility_id": station_ids,
                "commodity_category": COMMODITY_CATEGORY,
                "quantity": [float(inv0[sid]) for sid in station_ids],
                "quantity_unit": ["bike"] * len(station_ids),
            },
        )

        op_cap_rows = []
        for _, r in stations.iterrows():
            op_cap_rows.append(
                {
                    "facility_id": str(r["node_id"]),
                    "operation_type": "storage",
                    "commodity_category": COMMODITY_CATEGORY,
                    "capacity": float(r["inventory_capacity"]),
                    "capacity_unit": "bike",
                },
            )
        operation_capacities = pd.DataFrame(op_cap_rows)

        horizon_dates = [p["start_date"] for _, p in periods.iterrows()]
        cost_rows: list[dict] = []
        costs = self._source.df_station_costs
        if costs is not None and not costs.empty:
            for d in horizon_dates:
                for _, r in costs.iterrows():
                    sid = str(r["station_id"])
                    cost_rows.append(
                        {
                            "facility_id": sid,
                            "operation_type": "visit",
                            "commodity_category": COMMODITY_CATEGORY,
                            "date": d,
                            "cost_per_unit": float(r["fixed_cost_per_visit"]),
                            "cost_unit": "USD",
                        },
                    )
                    cost_rows.append(
                        {
                            "facility_id": sid,
                            "operation_type": "handling",
                            "commodity_category": COMMODITY_CATEGORY,
                            "date": d,
                            "cost_per_unit": float(r["cost_per_bike_moved"]),
                            "cost_unit": "USD",
                        },
                    )
        operation_costs = pd.DataFrame(cost_rows) if cost_rows else None

        home_depot = depot_ids[0]
        n_trucks = len(self._source.df_resources)
        resource_fleet = pd.DataFrame(
            {
                "facility_id": [home_depot],
                "resource_category": [RESOURCE_CATEGORY],
                "count": [n_trucks],
            },
        )

        resource_rows = []
        for _, r in self._source.df_resources.iterrows():
            resource_rows.append(
                {
                    "resource_id": str(r["resource_id"]),
                    "resource_category": RESOURCE_CATEGORY,
                    "home_facility_id": home_depot,
                    "capacity_override": float(r["capacity"]),
                    "description": None,
                },
            )
        resources_l3 = pd.DataFrame(resource_rows)

        rcost_rows: list[dict] = []
        tr = self._source.df_truck_rates
        if tr is not None and not tr.empty:
            for _, r in tr.iterrows():
                rcost_rows.append(
                    {
                        "resource_category": RESOURCE_CATEGORY,
                        "facility_id": home_depot,
                        "attribute_name": f"{r['resource_id']}_cost_per_km",
                        "date": None,
                        "value": float(r["cost_per_km"]),
                        "value_unit": "USD/km",
                    },
                )
                rcost_rows.append(
                    {
                        "resource_category": RESOURCE_CATEGORY,
                        "facility_id": home_depot,
                        "attribute_name": f"{r['resource_id']}_cost_per_hour",
                        "date": None,
                        "value": float(r["cost_per_hour"]),
                        "value_unit": "USD/h",
                    },
                )
                rcost_rows.append(
                    {
                        "resource_category": RESOURCE_CATEGORY,
                        "facility_id": home_depot,
                        "attribute_name": f"{r['resource_id']}_fixed_dispatch",
                        "date": None,
                        "value": float(r["fixed_dispatch_cost"]),
                        "value_unit": "USD",
                    },
                )
        resource_costs = pd.DataFrame(rcost_rows) if rcost_rows else None

        return RawModelData(
            facilities=facilities,
            commodity_categories=pd.DataFrame(
                {
                    "commodity_category_id": [COMMODITY_CATEGORY],
                    "name": ["Working bike"],
                    "unit": ["bike"],
                },
            ),
            resource_categories=pd.DataFrame(
                {
                    "resource_category_id": [RESOURCE_CATEGORY],
                    "name": ["Rebalancing truck"],
                    "base_capacity": [float(self._source.df_resources["capacity"].max())],
                    "capacity_unit": ["bike"],
                },
            ),
            planning_horizon=planning_horizon,
            planning_horizon_segments=planning_horizon_segments,
            periods=periods,
            facility_roles=facility_roles,
            facility_operations=facility_operations,
            edge_rules=edge_rules,
            resources=resources_l3,
            commodities=pd.DataFrame(
                {
                    "commodity_id": [COMMODITY_CATEGORY],
                    "commodity_category": [COMMODITY_CATEGORY],
                    "description": [""],
                },
            ),
            resource_commodity_compatibility=pd.DataFrame(
                {
                    "resource_category": [RESOURCE_CATEGORY],
                    "commodity_category": [COMMODITY_CATEGORY],
                    "enabled": [True],
                },
            ),
            resource_modal_compatibility=pd.DataFrame(
                {
                    "resource_category": [RESOURCE_CATEGORY],
                    "modal_type": [ModalType.ROAD.value],
                    "enabled": [True],
                },
            ),
            resource_fleet=resource_fleet,
            edges=edges_df,
            edge_commodities=ec_df,
            inventory_initial=inventory_initial,
            operation_capacities=operation_capacities,
            operation_costs=operation_costs,
            resource_costs=resource_costs,
        )


def _trip_flows_hourly(trips: pd.DataFrame) -> pd.DataFrame:
    if trips is None or trips.empty:
        return pd.DataFrame(columns=["source_id", "target_id", "period", "value"])
    t = trips.copy()
    return (
        t.assign(
            source_id=t["start_station_id"].astype(str),
            target_id=t["end_station_id"].astype(str),
            period=pd.to_datetime(t["started_at"]).dt.floor("h"),
        )
        .groupby(["source_id", "target_id", "period"], as_index=False)
        .size()
        .rename(columns={"size": "value"})
    )


def _telemetry_long(df: pd.DataFrame) -> pd.DataFrame:
    """Citi Bike-like wide telemetry → long [node_id, metric, timestamp, value]."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["node_id", "metric", "timestamp", "value"])
    metrics = [
        "num_bikes_available",
        "num_ebikes_available",
        "num_docks_available",
        "num_docks_disabled",
        "num_bikes_disabled",
    ]
    present = [m for m in metrics if m in df.columns]
    if not present:
        return pd.DataFrame(columns=["node_id", "metric", "timestamp", "value"])
    id_vars = ["timestamp", "station_id"]
    long = df[id_vars + present].melt(
        id_vars=id_vars,
        var_name="metric",
        value_name="value",
    )
    return long.rename(columns={"station_id": "node_id"})


def telemetry_long_from_source(df_telemetry_ts: pd.DataFrame) -> pd.DataFrame:
    """Public helper for notebooks: long telemetry from mock/CSV shape."""
    return _telemetry_long(df_telemetry_ts)
