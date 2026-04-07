"""Assemble ``RawModelData`` from a ``BikeShareSourceProtocol`` and run ``gbp.build.build_model``.

Trips and telemetry from the source are mapped to ``observed_flow`` and
``observed_inventory`` tables in the model.  Other wide tables (hourly
inventory matrix) remain on ``loader.source``.

Usage::

    from gbp.loaders import DataLoaderMock, DataLoaderGraph, GraphLoaderConfig

    mock = DataLoaderMock({"n_stations": 10})
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
    TripsSourceSchema,
)
from .protocols import BikeShareSourceProtocol

log = structlog.get_logger()

COMMODITY_CATEGORIES = ("electric_bike", "classic_bike")
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
    """Build ``RawModelData`` from a ``BikeShareSourceProtocol`` and run ``gbp.build.build_model``."""

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
        if self._config.build_observations:
            TripsSourceSchema.validate(self._source.df_trips)
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

        observations: dict[str, pd.DataFrame | None] = {}
        if self._config.build_observations:
            observations = self._build_observations(entities)
            obs_flow = observations.get("observed_flow")
            if obs_flow is not None and not obs_flow.empty:
                demand = self._build_demand_from_observations(obs_flow)
                if demand is not None:
                    observations["demand"] = demand
                supply = self._build_supply_from_observations(obs_flow)
                if supply is not None:
                    observations["supply"] = supply

        registry = AttributeRegistry()
        node_params = self._build_node_parameters(entities, registry)
        self._register_facility_costs(registry, temporal)
        self._register_resource_costs(registry, entities)

        all_tables = {
            **temporal,
            **entities.tables,
            **behavior,
            **edge_data,
            **node_params,
            **resources,
            **{k: v for k, v in observations.items() if v is not None},
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
            "facility_id": stations["station_id"].astype(str),
            "facility_type": "station",
            "name": stations["station_id"].astype(str),
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

        base_cap = float(self._source.df_resource_capacities["capacity"].max())

        tables: dict[str, pd.DataFrame] = {
            "facilities": facilities,
            "commodity_categories": pd.DataFrame({
                "commodity_category_id": list(COMMODITY_CATEGORIES),
                "name": ["Electric bike", "Classic bike"],
                "unit": ["bike", "bike"],
            }),
            "resource_categories": pd.DataFrame({
                "resource_category_id": [RESOURCE_CATEGORY],
                "name": ["Rebalancing truck"],
                "base_capacity": [base_cap],
                "capacity_unit": ["bike"],
            }),
            "commodities": pd.DataFrame({
                "commodity_id": list(COMMODITY_CATEGORIES),
                "commodity_category": list(COMMODITY_CATEGORIES),
                "description": ["", ""],
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
            base_pairs = [
                ("depot", "station"),
                ("station", "depot"),
                ("station", "station"),
                ("depot", "depot"),
            ]
            rule_rows: list[dict] = []
            for src_t, tgt_t in base_pairs:
                for cc in COMMODITY_CATEGORIES:
                    rule_rows.append({
                        "source_type": src_t,
                        "target_type": tgt_t,
                        "commodity_category": cc,
                        "modal_type": ModalType.ROAD.value,
                        "enabled": True,
                    })
            edge_rules = pd.DataFrame(rule_rows)
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
                for cc in COMMODITY_CATEGORIES:
                    ec_records.append({
                        "source_id": a,
                        "target_id": b,
                        "modal_type": ModalType.ROAD.value,
                        "commodity_category": cc,
                        "enabled": True,
                        "capacity_consumption": 1.0,
                    })

        return {
            "edges": pd.DataFrame(edge_records),
            "edge_commodities": pd.DataFrame(ec_records),
        }

    def _build_node_parameters(
        self,
        entities: _EntityResult,
        registry: AttributeRegistry,
    ) -> dict[str, pd.DataFrame]:
        """Initial inventory and storage capacities from source."""
        all_facility_ids = entities.station_ids + entities.depot_ids

        # ── Initial inventory from MultiIndex df_inventory_ts ────────
        inv0 = self._source.df_inventory_ts.iloc[0]
        inv_rows: list[dict] = []
        for fid in all_facility_ids:
            for cc in COMMODITY_CATEGORIES:
                qty = float(inv0.get((fid, cc), 0))
                inv_rows.append({
                    "facility_id": fid,
                    "commodity_category": cc,
                    "quantity": qty,
                    "quantity_unit": "bike",
                })
        inventory_initial = pd.DataFrame(inv_rows)

        # ── Storage capacities per commodity_category ────────────────
        cap_rows: list[dict] = []

        station_caps = self._source.df_station_capacities
        for _, r in station_caps.iterrows():
            cap_rows.append({
                "facility_id": str(r["station_id"]),
                "operation_type": "storage",
                "commodity_category": str(r["commodity_category"]),
                "capacity": float(r["capacity"]),
                "capacity_unit": "bike",
            })

        depot_caps = self._source.df_depot_capacities
        for _, r in depot_caps.iterrows():
            cap_rows.append({
                "facility_id": str(r["node_id"]),
                "operation_type": "storage",
                "commodity_category": str(r["commodity_category"]),
                "capacity": float(r["capacity"]),
                "capacity_unit": "bike",
            })

        registry.register(
            name="operation_capacity",
            data=pd.DataFrame(cap_rows),
            entity_type="facility",
            kind=AttributeKind.CAPACITY,
            grain=("facility_id", "operation_type", "commodity_category"),
            value_column="capacity",
            aggregation="min",
        )

        return {"inventory_initial": inventory_initial}

    def _register_facility_costs(
        self,
        registry: AttributeRegistry,
        temporal: dict[str, pd.DataFrame],
    ) -> None:
        """Register facility fixed costs (stations and depots) in the attribute registry."""
        periods = temporal["periods"]
        horizon_dates = [p["start_date"] for _, p in periods.iterrows()]

        cost_rows: list[dict] = []

        station_costs = self._source.df_station_costs
        if station_costs is not None and not station_costs.empty:
            for d in horizon_dates:
                for _, r in station_costs.iterrows():
                    cost_rows.append({
                        "facility_id": str(r["station_id"]),
                        "date": d,
                        "cost_per_unit": float(r["fixed_cost_station"]),
                        "cost_unit": "USD",
                    })

        depot_costs = self._source.df_depot_costs
        if depot_costs is not None and not depot_costs.empty:
            for d in horizon_dates:
                for _, r in depot_costs.iterrows():
                    cost_rows.append({
                        "facility_id": str(r["node_id"]),
                        "date": d,
                        "cost_per_unit": float(r["fixed_cost_depot"]),
                        "cost_unit": "USD",
                    })

        if cost_rows:
            registry.register(
                name="facility_fixed_cost",
                data=pd.DataFrame(cost_rows),
                entity_type="facility",
                kind=AttributeKind.COST,
                grain=("facility_id", "date"),
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

        cap_map = dict(zip(
            self._source.df_resource_capacities["resource_id"],
            self._source.df_resource_capacities["capacity"],
            strict=True,
        ))
        resource_rows = []
        for _, r in self._source.df_resources.iterrows():
            rid = str(r["resource_id"])
            resource_rows.append({
                "resource_id": rid,
                "resource_category": RESOURCE_CATEGORY,
                "home_facility_id": home_depot,
                "capacity_override": float(cap_map[rid]),
                "description": None,
            })

        n_cc = len(COMMODITY_CATEGORIES)
        return {
            "resource_fleet": resource_fleet,
            "resources": pd.DataFrame(resource_rows),
            "resource_commodity_compatibility": pd.DataFrame({
                "resource_category": [RESOURCE_CATEGORY] * n_cc,
                "commodity_category": list(COMMODITY_CATEGORIES),
                "enabled": [True] * n_cc,
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

    # ------------------------------------------------------------------
    # Internal — observations (trips / telemetry → observed_flow / observed_inventory)
    # ------------------------------------------------------------------

    def _build_observations(
        self, entities: _EntityResult,
    ) -> dict[str, pd.DataFrame | None]:
        """Map trips → observed_flow and telemetry → observed_inventory."""
        known_ids = set(entities.station_ids) | set(entities.depot_ids)
        result: dict[str, pd.DataFrame | None] = {}

        # ── trips → observed_flow ────────────────────────────────────
        df_trips = self._source.df_trips
        if df_trips is not None and not df_trips.empty:
            trips = df_trips[[
                "start_station_id", "end_station_id", "started_at", "rideable_type",
            ]].copy()
            trips = trips.rename(columns={
                "start_station_id": "source_id",
                "end_station_id": "target_id",
            })
            trips["date"] = pd.to_datetime(trips["started_at"]).dt.date
            trips["commodity_category"] = trips["rideable_type"]
            trips["quantity"] = 1.0
            trips["quantity_unit"] = "bike"
            trips["modal_type"] = None
            trips["resource_id"] = None

            mask = trips["source_id"].isin(known_ids) & trips["target_id"].isin(known_ids)
            trips = trips.loc[mask]

            if not trips.empty:
                grain = ["source_id", "target_id", "commodity_category", "date"]
                agg = trips.groupby(grain, as_index=False).agg(
                    quantity=("quantity", "sum"),
                    quantity_unit=("quantity_unit", "first"),
                    modal_type=("modal_type", "first"),
                    resource_id=("resource_id", "first"),
                )
                result["observed_flow"] = agg
                self._log.debug("observed_flow_built", rows=len(agg))
            else:
                result["observed_flow"] = None
        else:
            result["observed_flow"] = None

        # ── telemetry → observed_inventory (per commodity_category) ──
        df_tel = self._source.df_telemetry_ts
        if df_tel is not None and not df_tel.empty:
            tel_base = df_tel[["station_id", "timestamp", "num_bikes_available",
                               "num_ebikes_available"]].copy()

            # Electric bikes
            tel_e = tel_base[["station_id", "timestamp", "num_ebikes_available"]].copy()
            tel_e = tel_e.rename(columns={
                "station_id": "facility_id",
                "num_ebikes_available": "quantity",
            })
            tel_e["commodity_category"] = "electric_bike"

            # Classic bikes = total - ebike
            tel_c = tel_base[["station_id", "timestamp"]].copy()
            tel_c["quantity"] = (
                tel_base["num_bikes_available"] - tel_base["num_ebikes_available"]
            )
            tel_c = tel_c.rename(columns={"station_id": "facility_id"})
            tel_c["commodity_category"] = "classic_bike"

            tel = pd.concat([tel_e, tel_c], ignore_index=True)
            tel["date"] = pd.to_datetime(tel["timestamp"]).dt.date
            tel["quantity_unit"] = "bike"
            tel["quantity"] = tel["quantity"].astype(float)
            tel = tel.loc[tel["facility_id"].isin(known_ids)]

            if not tel.empty:
                grain = ["facility_id", "commodity_category", "date"]
                tel = tel.sort_values("timestamp")
                agg = tel.groupby(grain, as_index=False).agg(
                    quantity=("quantity", "last"),
                    quantity_unit=("quantity_unit", "first"),
                )
                result["observed_inventory"] = agg
                self._log.debug("observed_inventory_built", rows=len(agg))
            else:
                result["observed_inventory"] = None
        else:
            result["observed_inventory"] = None

        return result

    @staticmethod
    def _build_demand_from_observations(
        observed_flow: pd.DataFrame,
    ) -> pd.DataFrame | None:
        """Derive demand from observed_flow (departures per facility per date).

        Only organic flows (resource_id is null) are counted.
        """
        mask = observed_flow["resource_id"].isna()
        df = observed_flow.loc[mask]
        if df.empty:
            return None

        demand = (
            df.groupby(["source_id", "date", "commodity_category"], as_index=False)
            .agg(quantity=("quantity", "sum"), quantity_unit=("quantity_unit", "first"))
            .rename(columns={"source_id": "facility_id"})
        )
        return demand

    @staticmethod
    def _build_supply_from_observations(
        observed_flow: pd.DataFrame,
    ) -> pd.DataFrame | None:
        """Derive supply from observed_flow (arrivals per facility per date).

        Only organic flows (resource_id is null) are counted.
        """
        mask = observed_flow["resource_id"].isna()
        df = observed_flow.loc[mask]
        if df.empty:
            return None

        supply = (
            df.groupby(["target_id", "date", "commodity_category"], as_index=False)
            .agg(quantity=("quantity", "sum"), quantity_unit=("quantity_unit", "first"))
            .rename(columns={"target_id": "facility_id"})
        )
        return supply
