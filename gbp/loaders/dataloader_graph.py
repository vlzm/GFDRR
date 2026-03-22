"""Assemble ``RawModelData`` from a ``DataSourceProtocol`` and run ``gbp.build.build_model``.

This module only produces what lives in ``gbp.core`` / ``gbp.build``: validated
``RawModelData``, then ``ResolvedModelData``.  Extra wide tables from the source
(telemetry, trips, hourly inventory matrix, etc.) are **not** part of that model;
read them from ``loader.source`` (the same object passed into the constructor).

Usage::

    from gbp.loaders import DataLoaderMock, DataLoaderGraph, GraphLoaderConfig

    mock = DataLoaderMock({"n": 10})
    loader = DataLoaderGraph(mock, GraphLoaderConfig())
    loader.load_data()

    raw, resolved = loader.raw, loader.resolved
"""

from __future__ import annotations

import math

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
    def source(self) -> DataSourceProtocol:
        """Underlying data source (raw DataFrames, including non-core tables)."""
        return self._source

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
