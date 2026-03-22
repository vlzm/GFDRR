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
from typing import NamedTuple

import pandas as pd
import structlog

from gbp.build.pipeline import build_model
from gbp.core.attributes.registry import AttributeRegistry
from gbp.core.enums import AttributeKind, FacilityRole, ModalType, PeriodType
from gbp.core.model import RawModelData, ResolvedModelData

from .contracts import (
    DepotsSourceSchema,
    GraphLoaderConfig,
    ResourcesSourceSchema,
    StationsSourceSchema,
)
from .protocols import BikeShareSourceProtocol

log = structlog.get_logger()

COMMODITY_CATEGORY = "working_bike"
RESOURCE_CATEGORY = "rebalancing_truck"


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Internal data carrier for intermediate entity results
# ---------------------------------------------------------------------------

class _EntityResult(NamedTuple):
    """Intermediate result from ``_build_entities`` used by downstream builders."""
    tables: dict[str, pd.DataFrame]
    station_ids: list[str]
    depot_ids: list[str]


# ---------------------------------------------------------------------------
# DataLoaderGraph
# ---------------------------------------------------------------------------

class DataLoaderGraph:
    """Build ``RawModelData`` from a ``DataSourceProtocol`` and run ``gbp.build.build_model``."""

    def __init__(
        self,
        source: BikeShareSourceProtocol,
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
    def source(self) -> BikeShareSourceProtocol:
        """Underlying data source (raw DataFrames, including non-core tables)."""
        return self._source

    # ------------------------------------------------------------------
    # Internal — validation
    # ------------------------------------------------------------------

    def _validate_source(self) -> None:
        StationsSourceSchema.validate(self._source.df_stations)
        DepotsSourceSchema.validate(self._source.df_depots)
        ResourcesSourceSchema.validate(self._source.df_resources)
        self._log.debug("source_validated")

    # ------------------------------------------------------------------
    # Internal — raw model assembly (orchestrator + focused builders)
    # ------------------------------------------------------------------

    def _build_raw_model(self) -> RawModelData:
        """Assemble ``RawModelData`` from source DataFrames."""
        temporal = self._build_temporal()
        entities = self._build_entities()
        behavior = self._build_behavior(entities)
        edge_data = self._build_edges(entities) if self._config.build_edges else {}
        resources = self._build_resources(entities)

        registry = AttributeRegistry()
        flow_data = self._build_flow_data(entities, registry)
        self._register_costs(registry, temporal)
        self._register_resource_costs(registry, entities)

        all_tables = {
            **temporal,
            **entities.tables,
            **behavior,
            **edge_data,
            **flow_data,
            **resources,
        }
        return RawModelData(
            **{k: v for k, v in all_tables.items() if v is not None},
            attributes=registry,
        )

    def _build_temporal(self) -> dict[str, pd.DataFrame]:
        """Planning horizon, segments, and daily periods from source timestamps."""
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
            period_rows.append({
                "period_id": f"p{i}",
                "planning_horizon_id": "h1",
                "segment_index": 0,
                "period_index": i,
                "period_type": PeriodType.DAY.value,
                "start_date": d0,
                "end_date": d1,
            })

        return {
            "planning_horizon": pd.DataFrame({
                "planning_horizon_id": ["h1"],
                "name": ["mock_horizon"],
                "start_date": [start_d],
                "end_date": [end_d],
            }),
            "planning_horizon_segments": pd.DataFrame({
                "planning_horizon_id": ["h1"],
                "segment_index": [0],
                "start_date": [start_d],
                "end_date": [end_d],
                "period_type": PeriodType.DAY.value,
            }),
            "periods": pd.DataFrame(period_rows),
        }

    def _build_entities(self) -> _EntityResult:
        """Facilities, commodity/resource categories, and L3 items from source."""
        stations = self._source.df_stations
        depots = self._source.df_depots

        fac_stations = pd.DataFrame({
            "facility_id": stations["node_id"].astype(str),
            "facility_type": "station",
            "name": stations["name"] if "name" in stations.columns else stations["node_id"].astype(str),
            "lat": stations["lat"].astype(float),
            "lon": stations["lon"].astype(float),
        })
        fac_depots = pd.DataFrame({
            "facility_id": depots["node_id"].astype(str),
            "facility_type": "depot",
            "name": depots["node_id"].astype(str),
            "lat": depots["lat"].astype(float),
            "lon": depots["lon"].astype(float),
        })
        facilities = pd.concat([fac_depots, fac_stations], ignore_index=True)

        tables: dict[str, pd.DataFrame] = {
            "facilities": facilities,
            "commodity_categories": pd.DataFrame({
                "commodity_category_id": [COMMODITY_CATEGORY],
                "name": ["Working bike"],
                "unit": ["bike"],
            }),
            "resource_categories": pd.DataFrame({
                "resource_category_id": [RESOURCE_CATEGORY],
                "name": ["Rebalancing truck"],
                "base_capacity": [float(self._source.df_resources["capacity"].max())],
                "capacity_unit": ["bike"],
            }),
            "commodities": pd.DataFrame({
                "commodity_id": [COMMODITY_CATEGORY],
                "commodity_category": [COMMODITY_CATEGORY],
                "description": [""],
            }),
        }
        return _EntityResult(
            tables=tables,
            station_ids=list(fac_stations["facility_id"]),
            depot_ids=list(fac_depots["facility_id"]),
        )

    def _build_behavior(self, entities: _EntityResult) -> dict[str, pd.DataFrame]:
        """Facility roles, operations, and edge generation rules."""
        role_rows: list[dict] = []
        op_rows: list[dict] = []

        for did in entities.depot_ids:
            role_rows.extend([
                {"facility_id": did, "role": FacilityRole.STORAGE.value},
                {"facility_id": did, "role": FacilityRole.TRANSSHIPMENT.value},
            ])
            op_rows.extend([
                {"facility_id": did, "operation_type": "receiving", "enabled": True},
                {"facility_id": did, "operation_type": "storage", "enabled": True},
                {"facility_id": did, "operation_type": "dispatch", "enabled": True},
            ])

        for sid in entities.station_ids:
            role_rows.extend([
                {"facility_id": sid, "role": FacilityRole.SINK.value},
                {"facility_id": sid, "role": FacilityRole.STORAGE.value},
                {"facility_id": sid, "role": FacilityRole.SOURCE.value},
            ])
            op_rows.extend([
                {"facility_id": sid, "operation_type": "receiving", "enabled": True},
                {"facility_id": sid, "operation_type": "storage", "enabled": True},
                {"facility_id": sid, "operation_type": "dispatch", "enabled": True},
            ])

        if self._config.build_edges:
            edge_rules = pd.DataFrame({
                "source_type": ["depot", "station", "station", "depot"],
                "target_type": ["station", "depot", "station", "depot"],
                "commodity_category": [COMMODITY_CATEGORY] * 4,
                "modal_type": [ModalType.ROAD.value] * 4,
                "enabled": [True] * 4,
            })
        else:
            edge_rules = pd.DataFrame({
                "source_type": pd.Series(dtype="string"),
                "target_type": pd.Series(dtype="string"),
                "commodity_category": pd.Series(dtype="string"),
                "modal_type": pd.Series(dtype="string"),
                "enabled": pd.Series(dtype="bool"),
            })

        return {
            "facility_roles": pd.DataFrame(role_rows),
            "facility_operations": pd.DataFrame(op_rows),
            "edge_rules": edge_rules,
        }

    def _build_edges(self, entities: _EntityResult) -> dict[str, pd.DataFrame]:
        """All-pairs edges with distance computation and commodity mapping."""
        facilities = entities.tables["facilities"]
        latlon = {
            str(r["facility_id"]): (float(r["lat"]), float(r["lon"]))
            for _, r in facilities.iterrows()
        }
        ids = list(facilities["facility_id"].astype(str))
        speed = self._config.default_speed_kmh

        edge_records: list[dict] = []
        ec_records: list[dict] = []
        for i, a in enumerate(ids):
            la0, lo0 = latlon[a]
            for j, b in enumerate(ids):
                if i == j:
                    continue
                la1, lo1 = latlon[b]
                dkm = _pair_distance_km(la0, lo0, la1, lo1, self._config.distance_backend)
                lt_h = dkm / speed if speed > 0 else 0.0
                edge_records.append({
                    "source_id": a,
                    "target_id": b,
                    "modal_type": ModalType.ROAD.value,
                    "distance": dkm,
                    "distance_unit": "km",
                    "lead_time_hours": max(lt_h, 1e-6),
                    "reliability": None,
                })
                ec_records.append({
                    "source_id": a,
                    "target_id": b,
                    "modal_type": ModalType.ROAD.value,
                    "commodity_category": COMMODITY_CATEGORY,
                    "enabled": True,
                    "capacity_consumption": 1.0,
                })

        return {
            "edges": pd.DataFrame(edge_records),
            "edge_commodities": pd.DataFrame(ec_records),
        }

    def _build_flow_data(
        self,
        entities: _EntityResult,
        registry: AttributeRegistry,
    ) -> dict[str, pd.DataFrame]:
        """Initial inventory and operation capacities from source."""
        station_ids = entities.station_ids
        stations = self._source.df_stations

        inv0 = self._source.df_inventory_ts.iloc[0]
        inventory_initial = pd.DataFrame({
            "facility_id": station_ids,
            "commodity_category": COMMODITY_CATEGORY,
            "quantity": [float(inv0[sid]) for sid in station_ids],
            "quantity_unit": ["bike"] * len(station_ids),
        })

        op_cap_rows = []
        for _, r in stations.iterrows():
            op_cap_rows.append({
                "facility_id": str(r["node_id"]),
                "operation_type": "storage",
                "commodity_category": COMMODITY_CATEGORY,
                "capacity": float(r["inventory_capacity"]),
                "capacity_unit": "bike",
            })

        registry.register(
            name="operation_capacity",
            data=pd.DataFrame(op_cap_rows),
            entity_type="facility",
            kind=AttributeKind.CAPACITY,
            grain=("facility_id", "operation_type", "commodity_category"),
            value_column="capacity",
            aggregation="min",
        )

        return {"inventory_initial": inventory_initial}

    def _register_costs(
        self,
        registry: AttributeRegistry,
        temporal: dict[str, pd.DataFrame],
    ) -> None:
        """Register operation costs (station visit/handling) in the attribute registry."""
        periods = temporal["periods"]
        horizon_dates = [p["start_date"] for _, p in periods.iterrows()]

        cost_rows: list[dict] = []
        costs = self._source.df_station_costs
        if costs is not None and not costs.empty:
            for d in horizon_dates:
                for _, r in costs.iterrows():
                    sid = str(r["station_id"])
                    cost_rows.append({
                        "facility_id": sid,
                        "operation_type": "visit",
                        "commodity_category": COMMODITY_CATEGORY,
                        "date": d,
                        "cost_per_unit": float(r["fixed_cost_per_visit"]),
                        "cost_unit": "USD",
                    })
                    cost_rows.append({
                        "facility_id": sid,
                        "operation_type": "handling",
                        "commodity_category": COMMODITY_CATEGORY,
                        "date": d,
                        "cost_per_unit": float(r["cost_per_bike_moved"]),
                        "cost_unit": "USD",
                    })

        if cost_rows:
            registry.register(
                name="operation_cost",
                data=pd.DataFrame(cost_rows),
                entity_type="facility",
                kind=AttributeKind.COST,
                grain=("facility_id", "operation_type", "commodity_category", "date"),
                value_column="cost_per_unit",
                aggregation="mean",
                unit="USD",
            )

    def _build_resources(self, entities: _EntityResult) -> dict[str, pd.DataFrame | None]:
        """Resource fleet, L3 resources, and compatibility tables."""
        home_depot = entities.depot_ids[0]

        resource_fleet = pd.DataFrame({
            "facility_id": [home_depot],
            "resource_category": [RESOURCE_CATEGORY],
            "count": [len(self._source.df_resources)],
        })

        resource_rows = []
        for _, r in self._source.df_resources.iterrows():
            resource_rows.append({
                "resource_id": str(r["resource_id"]),
                "resource_category": RESOURCE_CATEGORY,
                "home_facility_id": home_depot,
                "capacity_override": float(r["capacity"]),
                "description": None,
            })

        return {
            "resource_fleet": resource_fleet,
            "resources": pd.DataFrame(resource_rows),
            "resource_commodity_compatibility": pd.DataFrame({
                "resource_category": [RESOURCE_CATEGORY],
                "commodity_category": [COMMODITY_CATEGORY],
                "enabled": [True],
            }),
            "resource_modal_compatibility": pd.DataFrame({
                "resource_category": [RESOURCE_CATEGORY],
                "modal_type": [ModalType.ROAD.value],
                "enabled": [True],
            }),
        }

    def _register_resource_costs(
        self,
        registry: AttributeRegistry,
        entities: _EntityResult,
    ) -> None:
        """Register per-resource cost attributes (cost_per_km, cost_per_hour, fixed_dispatch)."""
        home_depot = entities.depot_ids[0]
        tr = self._source.df_truck_rates
        if tr is None or tr.empty:
            return

        for cost_attr, col in [
            ("resource_cost_per_km", "cost_per_km"),
            ("resource_cost_per_hour", "cost_per_hour"),
            ("resource_fixed_dispatch", "fixed_dispatch_cost"),
        ]:
            rows = []
            for _, r in tr.iterrows():
                rows.append({
                    "resource_category": RESOURCE_CATEGORY,
                    "facility_id": home_depot,
                    "resource_id": str(r["resource_id"]),
                    "value": float(r[col]),
                })
            if rows:
                registry.register(
                    name=cost_attr,
                    data=pd.DataFrame(rows),
                    entity_type="resource",
                    kind=AttributeKind.COST,
                    grain=("resource_category", "facility_id", "resource_id"),
                    value_column="value",
                    aggregation="mean",
                    unit="USD",
                )
