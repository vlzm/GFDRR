"""Assemble ``RawModelData`` from a ``BikeShareSourceProtocol``.

Trips and telemetry from the source are mapped to ``observed_flow`` and
``observed_inventory`` tables in the model.  Other wide tables (hourly
inventory matrix) remain on ``loader.source``.

Usage::

    from gbp.build.pipeline import build_model
    from gbp.loaders import DataLoaderGraph, DataLoaderMock, GraphLoaderConfig

    mock = DataLoaderMock({"n_stations": 10})
    loader = DataLoaderGraph(mock, GraphLoaderConfig())
    raw = loader.load()
    resolved = build_model(raw)
"""

from __future__ import annotations

import math
from typing import NamedTuple

import pandas as pd
import structlog

from gbp.build.defaults import DEFAULT_COMMODITY_CATEGORY_ID
from gbp.core.attributes.registry import AttributeRegistry
from gbp.core.enums import AttributeKind, ModalType, PeriodType
from gbp.core.model import RawModelData

from .contracts import (
    DepotsSourceSchema,
    GraphLoaderConfig,
    ResourcesSourceSchema,
    StationsSourceSchema,
    TripsSourceSchema,
)
from .protocols import BikeShareSourceProtocol

log = structlog.get_logger()

# Bike-share canonical commodity categories used by ``DataLoaderMock``.
# ``DataLoaderGraph`` itself discovers categories from trips via
# ``_commodity_categories()``; this tuple exists for tests and documentation
# that reference the canonical bike-share taxonomy.
COMMODITY_CATEGORIES = ("electric_bike", "classic_bike")
RESOURCE_CATEGORY = "rebalancing_truck"


def _nonempty_df(source: BikeShareSourceProtocol, attr: str) -> pd.DataFrame | None:
    """Return ``getattr(source, attr)`` if it is a non-empty DataFrame.

    Optional source tables are allowed to be ``None`` *or* missing entirely;
    empty DataFrames are treated as "no rows" and collapsed to ``None`` so
    downstream code can use a single ``is None`` check.

    Parameters
    ----------
    source
        Data source implementing ``BikeShareSourceProtocol``.
    attr
        Attribute name to look up on *source*.

    Returns
    -------
    pd.DataFrame or None
        The DataFrame when present and non-empty, otherwise ``None``.
    """
    val = getattr(source, attr, None)
    if val is None:
        return None
    if isinstance(val, pd.DataFrame) and val.empty:
        return None
    return val


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute great-circle distance between two points in kilometres."""
    rlat1, rlon1 = math.radians(lat1), math.radians(lon1)
    rlat2, rlon2 = math.radians(lat2), math.radians(lon2)
    dlat, dlon = rlat2 - rlat1, rlon2 - rlon1
    h = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return 2 * 6_371.0 * math.asin(min(1.0, math.sqrt(h)))


def _euclidean_latlon_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute approximate Euclidean distance between two lat/lon points in kilometres."""
    km_per_deg_lat = 111.0
    km_per_deg_lon = 111.0 * math.cos(math.radians((lat1 + lat2) / 2))
    dx = (lon2 - lon1) * km_per_deg_lon
    dy = (lat2 - lat1) * km_per_deg_lat
    return math.sqrt(dx * dx + dy * dy)


def _pair_distance_km(
    lat1: float, lon1: float, lat2: float, lon2: float, backend: str,
) -> float:
    """Dispatch pairwise distance calculation to the chosen backend."""
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
    """Assemble ``RawModelData`` from a ``BikeShareSourceProtocol``.

    The loader covers only the source-to-raw transition.  Call
    ``gbp.build.pipeline.build_model`` explicitly to produce the resolved model.

    Parameters
    ----------
    source
        Bike-sharing data source that satisfies ``BikeShareSourceProtocol``.
    config
        Loader configuration. When ``None``, defaults from
        ``GraphLoaderConfig`` are used.
    """

    def __init__(
        self,
        source: BikeShareSourceProtocol,
        config: GraphLoaderConfig | None = None,
    ) -> None:
        self._source = source
        self._config = config or GraphLoaderConfig()
        self._log = log.bind(loader="graph_core")
        self._raw: RawModelData | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self) -> RawModelData:
        """Load source tables and assemble ``RawModelData``.

        Validation and derivation of optional tables happen later, inside
        ``build_model``.  The returned raw model is cached on the loader and
        also accessible via the ``raw`` property.

        Returns
        -------
        RawModelData
            Assembled raw model data.
        """
        self._log.info("load_start")
        self._source.load_data()
        self._validate_source()

        self._raw = self._build_raw_model()
        self._log.info("load_done", facilities=len(self._raw.facilities))
        return self._raw

    @property
    def raw(self) -> RawModelData:
        """Return cached ``RawModelData``.

        Raises
        ------
        ValueError
            If ``load()`` has not been called yet.
        """
        if self._raw is None:
            raise ValueError("Data is not loaded. Call load() first.")
        return self._raw

    @property
    def available_dates(self) -> pd.DatetimeIndex:
        """Return the timestamp index from the underlying source."""
        return self._source.timestamps

    @property
    def source(self) -> BikeShareSourceProtocol:
        """Return the underlying data source (raw DataFrames, including non-core tables)."""
        return self._source

    # ------------------------------------------------------------------
    # Internal — validation
    # ------------------------------------------------------------------

    def _validate_source(self) -> None:
        """Validate source shape with Pandera schemas."""
        StationsSourceSchema.validate(self._source.df_stations)
        if self._config.build_observations:
            TripsSourceSchema.validate(self._source.df_trips)

        depots = _nonempty_df(self._source, "df_depots")
        if depots is not None:
            DepotsSourceSchema.validate(depots)

        resources = _nonempty_df(self._source, "df_resources")
        if resources is not None:
            ResourcesSourceSchema.validate(resources)

        self._log.debug("source_validated")

    # ------------------------------------------------------------------
    # Internal — raw model assembly (orchestrator + focused builders)
    # ------------------------------------------------------------------

    def _build_raw_model(self) -> RawModelData:
        """Assemble ``RawModelData`` from source DataFrames.

        Returns
        -------
        RawModelData
            Fully assembled (but not yet resolved) model data.
        """
        temporal = self._build_temporal()
        entities = self._build_entities()
        behavior = self._build_behavior(entities)
        distance_data = self._build_distance_matrix(entities) if self._config.build_edges else {}
        resources = self._build_resources(entities)

        observations: dict[str, pd.DataFrame | None] = {}
        if self._config.build_observations:
            observations = self._build_observations(entities)

        registry = AttributeRegistry()
        node_params = self._build_node_parameters(registry)
        self._register_facility_costs(registry, temporal)
        self._register_resource_costs(registry, entities)

        all_tables = {
            **temporal,
            **entities.tables,
            **behavior,
            **distance_data,
            **node_params,
            **resources,
            **{k: v for k, v in observations.items() if v is not None},
        }
        return RawModelData(
            **{k: v for k, v in all_tables.items() if v is not None},
            attributes=registry,
        )

    def _build_temporal(self) -> dict[str, pd.DataFrame]:
        """Build planning horizon and single daily segment covering the source time range.

        Prefers ``source.timestamps`` when available.  Falls back to the date
        range of ``df_trips["started_at"]`` for minimal sources that don't carry
        an explicit timestamp index.

        Returns
        -------
        dict[str, pd.DataFrame]
            ``planning_horizon`` and ``planning_horizon_segments`` tables.
        """
        start_d, end_d = self._derive_horizon_dates()

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
        }

    def _derive_horizon_dates(self) -> tuple:
        """Derive ``(start_date, end_date)`` for the planning horizon.

        Uses ``source.timestamps`` when present.  Otherwise derives the span
        from ``df_trips["started_at"]``.

        Returns
        -------
        tuple
            ``(start_date, end_date)`` as ``datetime.date`` objects.

        Raises
        ------
        ValueError
            If neither ``timestamps`` nor ``df_trips["started_at"]`` is available.
        """
        ts = getattr(self._source, "timestamps", None)
        if ts is not None and len(ts) > 0:
            start_d = pd.Timestamp(ts[0]).normalize().date()
            end_d = (pd.Timestamp(ts[-1]).normalize() + pd.Timedelta(days=1)).date()
            return start_d, end_d

        trips = _nonempty_df(self._source, "df_trips")
        if trips is not None and "started_at" in trips.columns:
            started = pd.to_datetime(trips["started_at"])
            start_d = started.min().normalize().date()
            end_d = (started.max().normalize() + pd.Timedelta(days=1)).date()
            return start_d, end_d

        raise ValueError(
            "Cannot derive planning horizon: source has neither `timestamps` "
            "nor a non-empty `df_trips` with `started_at`."
        )

    def _build_entities(self) -> _EntityResult:
        """Build facilities and commodity/resource categories from the source.

        Depots, resource categories, and explicit commodities are all optional.
        When absent, the loader either emits a default placeholder or leaves
        the table out entirely for ``build_model`` to derive.

        Returns
        -------
        _EntityResult
            Intermediate carrier with tables, station ids, and depot ids.
        """
        stations = self._source.df_stations
        fac_stations = pd.DataFrame({
            "facility_id": stations["station_id"].astype(str),
            "facility_type": "station",
            "name": stations["station_id"].astype(str),
            "lat": stations["lat"].astype(float),
            "lon": stations["lon"].astype(float),
        })

        depots = _nonempty_df(self._source, "df_depots")
        if depots is not None:
            fac_depots = pd.DataFrame({
                "facility_id": depots["node_id"].astype(str),
                "facility_type": "depot",
                "name": depots["node_id"].astype(str),
                "lat": depots["lat"].astype(float),
                "lon": depots["lon"].astype(float),
            })
            facilities = pd.concat([fac_depots, fac_stations], ignore_index=True)
            depot_ids = list(fac_depots["facility_id"])
        else:
            facilities = fac_stations.reset_index(drop=True)
            depot_ids = []

        tables: dict[str, pd.DataFrame] = {"facilities": facilities}

        commodity_cats = self._commodity_categories()
        tables["commodity_categories"] = pd.DataFrame({
            "commodity_category_id": list(commodity_cats),
            "name": [cc.replace("_", " ").capitalize() for cc in commodity_cats],
            "unit": ["unit"] * len(commodity_cats),
        })
        tables["commodities"] = pd.DataFrame({
            "commodity_id": list(commodity_cats),
            "commodity_category": list(commodity_cats),
            "description": [""] * len(commodity_cats),
        })

        res_caps = _nonempty_df(self._source, "df_resource_capacities")
        if res_caps is not None:
            tables["resource_categories"] = pd.DataFrame({
                "resource_category_id": [RESOURCE_CATEGORY],
                "name": ["Rebalancing truck"],
                "base_capacity": [float(res_caps["capacity"].max())],
            })

        return _EntityResult(
            tables=tables,
            station_ids=list(fac_stations["facility_id"]),
            depot_ids=depot_ids,
        )

    def _commodity_categories(self) -> tuple[str, ...]:
        """Discover commodity categories from ``df_trips.rideable_type`` if present.

        Falls back to a single ``DEFAULT_COMMODITY_CATEGORY_ID`` when trips
        carry no explicit type.

        Returns
        -------
        tuple[str, ...]
            Sorted tuple of commodity category identifiers.
        """
        trips = _nonempty_df(self._source, "df_trips")
        if trips is not None and "rideable_type" in trips.columns:
            cats = tuple(
                sorted({str(v) for v in trips["rideable_type"].dropna().unique()})
            )
            if cats:
                return cats
        return (DEFAULT_COMMODITY_CATEGORY_ID,)

    def _build_behavior(self, entities: _EntityResult) -> dict[str, pd.DataFrame]:
        """Build facility operations and edge generation rules.

        Roles are derived by ``build_model`` from ``(facility_type, operations)``
        via ``gbp.core.roles.derive_roles``; the loader intentionally does
        not emit a ``facility_roles`` table.

        Edge rules cover station-to-station always; depot pairs are only
        emitted when the source actually has depots.

        Parameters
        ----------
        entities
            Intermediate entity result from ``_build_entities``.

        Returns
        -------
        dict[str, pd.DataFrame]
            ``facility_operations`` and ``edge_rules`` tables.
        """
        op_rows: list[dict] = []

        for did in entities.depot_ids:
            op_rows.extend([
                {"facility_id": did, "operation_type": "receiving", "enabled": True},
                {"facility_id": did, "operation_type": "storage", "enabled": True},
                {"facility_id": did, "operation_type": "dispatch", "enabled": True},
            ])

        for sid in entities.station_ids:
            op_rows.extend([
                {"facility_id": sid, "operation_type": "receiving", "enabled": True},
                {"facility_id": sid, "operation_type": "storage", "enabled": True},
                {"facility_id": sid, "operation_type": "dispatch", "enabled": True},
            ])

        if self._config.build_edges:
            base_pairs = [("station", "station")]
            if entities.depot_ids:
                base_pairs.extend([
                    ("depot", "station"),
                    ("station", "depot"),
                    ("depot", "depot"),
                ])
            commodity_cats = self._commodity_categories()
            rule_rows: list[dict] = []
            for src_t, tgt_t in base_pairs:
                for cc in commodity_cats:
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
            "facility_operations": pd.DataFrame(op_rows),
            "edge_rules": edge_rules,
        }

    def _build_distance_matrix(self, entities: _EntityResult) -> dict[str, pd.DataFrame]:
        """Compute all-pairs pairwise distances and travel durations between facilities.

        Parameters
        ----------
        entities
            Intermediate entity result from ``_build_entities``.

        Returns
        -------
        dict[str, pd.DataFrame]
            Single-key dict with the ``distance_matrix`` table.
        """
        facilities = entities.tables["facilities"]
        latlon = {
            str(r["facility_id"]): (float(r["lat"]), float(r["lon"]))
            for _, r in facilities.iterrows()
        }
        ids = list(facilities["facility_id"].astype(str))
        speed = self._config.default_speed_kmh

        records: list[dict] = []
        for i, a in enumerate(ids):
            la0, lo0 = latlon[a]
            for j, b in enumerate(ids):
                if i == j:
                    continue
                la1, lo1 = latlon[b]
                dkm = _pair_distance_km(la0, lo0, la1, lo1, self._config.distance_backend)
                dur = dkm / speed if speed > 0 else 0.0
                records.append({
                    "source_id": a,
                    "target_id": b,
                    "distance": dkm,
                    "duration": max(dur, 1e-6),
                })

        return {"distance_matrix": pd.DataFrame(records)}

    def _build_node_parameters(
        self,
        registry: AttributeRegistry,
    ) -> dict[str, pd.DataFrame]:
        """Build initial inventory and register storage capacity attributes.

        Both are optional in minimal mode:

        - ``inventory_initial`` is taken directly from the source when present.
          Otherwise ``build_model`` seeds it from observed flow (or leaves it
          empty if flow is also absent).
        - Storage capacity attributes are registered only when the source has
          station / depot capacities.

        Parameters
        ----------
        registry
            Attribute registry where capacity attributes are registered.

        Returns
        -------
        dict[str, pd.DataFrame]
            May contain ``inventory_initial``; empty dict when absent.
        """
        result: dict[str, pd.DataFrame] = {}

        inv_initial = _nonempty_df(self._source, "inventory_initial")
        if inv_initial is not None:
            result["inventory_initial"] = inv_initial.copy()

        cap_rows: list[dict] = []
        station_caps = _nonempty_df(self._source, "df_station_capacities")
        if station_caps is not None:
            for _, r in station_caps.iterrows():
                cap_rows.append({
                    "facility_id": str(r["station_id"]),
                    "operation_type": "storage",
                    "commodity_category": str(r["commodity_category"]),
                    "capacity": float(r["capacity"]),
                })

        depot_caps = _nonempty_df(self._source, "df_depot_capacities")
        if depot_caps is not None:
            for _, r in depot_caps.iterrows():
                cap_rows.append({
                    "facility_id": str(r["node_id"]),
                    "operation_type": "storage",
                    "commodity_category": str(r["commodity_category"]),
                    "capacity": float(r["capacity"]),
                })

        if cap_rows:
            registry.register(
                name="operation_capacity",
                data=pd.DataFrame(cap_rows),
                entity_type="facility",
                kind=AttributeKind.CAPACITY,
                grain=("facility_id", "operation_type", "commodity_category"),
                value_column="capacity",
                aggregation="min",
            )

        return result

    def _register_facility_costs(
        self,
        registry: AttributeRegistry,
        temporal: dict[str, pd.DataFrame],
    ) -> None:
        """Register facility fixed costs (stations and depots) in the attribute registry.

        Costs are per-day; the horizon is taken from ``planning_horizon`` so
        this works even for minimal sources without ``timestamps``.

        Parameters
        ----------
        registry
            Attribute registry to register costs into.
        temporal
            Dict containing the ``planning_horizon`` table.
        """
        station_costs = _nonempty_df(self._source, "df_station_costs")
        depot_costs = _nonempty_df(self._source, "df_depot_costs")
        if station_costs is None and depot_costs is None:
            return

        horizon = temporal["planning_horizon"].iloc[0]
        start = pd.Timestamp(horizon["start_date"])
        end = pd.Timestamp(horizon["end_date"]) - pd.Timedelta(days=1)
        horizon_dates = [d.date() for d in pd.date_range(start=start, end=end, freq="D")]

        cost_rows: list[dict] = []
        if station_costs is not None:
            for d in horizon_dates:
                for _, r in station_costs.iterrows():
                    cost_rows.append({
                        "facility_id": str(r["station_id"]),
                        "date": d,
                        "cost_per_unit": float(r["fixed_cost_station"]),
                        "cost_unit": "USD",
                    })

        if depot_costs is not None:
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
        """Build resource fleet and compatibility tables.

        Skipped entirely when the source has no depots (no home for the fleet)
        or no ``df_resources`` -- returns ``{}`` in that case.

        Parameters
        ----------
        entities
            Intermediate entity result from ``_build_entities``.

        Returns
        -------
        dict[str, pd.DataFrame | None]
            Resource-related tables or empty dict when not applicable.
        """
        if not entities.depot_ids:
            return {}

        resources_src = _nonempty_df(self._source, "df_resources")
        res_caps_src = _nonempty_df(self._source, "df_resource_capacities")
        if resources_src is None or res_caps_src is None:
            return {}

        home_depot = entities.depot_ids[0]

        resource_fleet = pd.DataFrame({
            "facility_id": [home_depot],
            "resource_category": [RESOURCE_CATEGORY],
            "count": [len(resources_src)],
        })

        cap_map = dict(zip(
            res_caps_src["resource_id"],
            res_caps_src["capacity"],
            strict=True,
        ))
        resource_rows = []
        for _, r in resources_src.iterrows():
            rid = str(r["resource_id"])
            resource_rows.append({
                "resource_id": rid,
                "resource_category": RESOURCE_CATEGORY,
                "home_facility_id": home_depot,
                "capacity_override": float(cap_map[rid]),
                "description": None,
            })

        commodity_cats = self._commodity_categories()
        n_cc = len(commodity_cats)
        return {
            "resource_fleet": resource_fleet,
            "resources": pd.DataFrame(resource_rows),
            "resource_commodity_compatibility": pd.DataFrame({
                "resource_category": [RESOURCE_CATEGORY] * n_cc,
                "commodity_category": list(commodity_cats),
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
        """Register per-resource cost attributes in the attribute registry.

        Registers ``resource_cost_per_km``, ``resource_cost_per_hour``, and
        ``resource_fixed_dispatch`` when the source provides truck rates.

        Parameters
        ----------
        registry
            Attribute registry to register costs into.
        entities
            Intermediate entity result from ``_build_entities``.
        """
        if not entities.depot_ids:
            return
        tr = _nonempty_df(self._source, "df_truck_rates")
        if tr is None:
            return
        home_depot = entities.depot_ids[0]

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
        """Map trips to ``observed_flow`` and telemetry to ``observed_inventory``.

        When trips carry no ``rideable_type`` column, the flow is labelled
        with ``DEFAULT_COMMODITY_CATEGORY_ID`` so it lines up with the default
        single-category table emitted by ``_build_entities``.

        Parameters
        ----------
        entities
            Intermediate entity result from ``_build_entities``.

        Returns
        -------
        dict[str, pd.DataFrame | None]
            ``observed_flow`` and ``observed_inventory`` (either may be ``None``).
        """
        known_ids = set(entities.station_ids) | set(entities.depot_ids)
        result: dict[str, pd.DataFrame | None] = {}

        # ── trips → observed_flow ────────────────────────────────────
        df_trips = _nonempty_df(self._source, "df_trips")
        if df_trips is not None:
            has_ended_at = "ended_at" in df_trips.columns
            keep_cols = ["start_station_id", "end_station_id", "started_at"]
            if has_ended_at:
                keep_cols = [*keep_cols, "ended_at"]
            trips = df_trips[keep_cols].copy()
            trips = trips.rename(columns={
                "start_station_id": "source_id",
                "end_station_id": "target_id",
            })
            trips["date"] = pd.to_datetime(trips["started_at"]).dt.date
            if has_ended_at:
                trips["duration_hours"] = (
                    pd.to_datetime(trips["ended_at"])
                    - pd.to_datetime(trips["started_at"])
                ).dt.total_seconds() / 3600.0
                trips = trips.drop(columns=["ended_at"])
            else:
                trips["duration_hours"] = float("nan")
            if "rideable_type" in df_trips.columns:
                trips["commodity_category"] = df_trips["rideable_type"].astype(str).values
            else:
                trips["commodity_category"] = DEFAULT_COMMODITY_CATEGORY_ID
            trips["quantity"] = 1.0
            trips["modal_type"] = None
            trips["resource_id"] = None

            mask = trips["source_id"].isin(known_ids) & trips["target_id"].isin(known_ids)
            trips = trips.loc[mask]

            if not trips.empty:
                grain = ["source_id", "target_id", "commodity_category", "date"]
                agg = trips.groupby(grain, as_index=False).agg(
                    quantity=("quantity", "sum"),
                    duration_hours=("duration_hours", "mean"),
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
        df_tel = _nonempty_df(self._source, "df_telemetry_ts")
        if df_tel is not None:
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
            tel["quantity"] = tel["quantity"].astype(float)
            tel = tel.loc[tel["facility_id"].isin(known_ids)]

            if not tel.empty:
                grain = ["facility_id", "commodity_category", "date"]
                tel = tel.sort_values("timestamp")
                agg = tel.groupby(grain, as_index=False).agg(
                    quantity=("quantity", "last"),
                )
                result["observed_inventory"] = agg
                self._log.debug("observed_inventory_built", rows=len(agg))
            else:
                result["observed_inventory"] = None
        else:
            result["observed_inventory"] = None

        return result

