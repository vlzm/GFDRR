"""Graph data loader — converts raw source data into a universal GraphData.

Static parts (nodes, coordinates, resources, commodities, attributes) are
built once.  Inventory varies over time; call ``get_snapshot(date)`` to obtain
a ``GraphData`` for a specific moment.

Usage::

    from gbp.loaders import DataLoaderMock, DataLoaderGraph, GraphLoaderConfig

    mock = DataLoaderMock({"n": 10})
    loader = DataLoaderGraph(mock, GraphLoaderConfig())
    loader.load_data()

    snapshot = loader.get_snapshot(pd.Timestamp("2025-01-03 12:00"))
"""

from __future__ import annotations

import structlog
import pandas as pd

from gbp.graph import (
    AttributeTable,
    DistanceService,
    EdgeBuilder,
    FlowsTable,
    GraphData,
)

from .contracts import (
    DepotsSourceSchema,
    GraphLoaderConfig,
    ResourcesSourceSchema,
    StationsSourceSchema,
)
from .protocols import DataSourceProtocol

log = structlog.get_logger()


class DataLoaderGraph:
    """Convert raw source data into a universal ``GraphData``.

    Static parts (nodes, edges, resources, commodities, node attributes) are
    built once in ``load_data()``.  Temporal inventory is stored internally;
    ``get_snapshot(date)`` returns a complete graph for a given timestamp.
    """

    def __init__(
        self,
        source: DataSourceProtocol,
        config: GraphLoaderConfig | None = None,
    ) -> None:
        self._source = source
        self._config = config or GraphLoaderConfig()
        self._log = log.bind(loader="graph")
        self._graph: GraphData | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_data(self) -> None:
        """Load source data and build all static graph parts."""
        self._log.info("load_start")

        self._source.load_data()
        self._validate_source()

        nodes = self._build_nodes()
        coordinates = self._build_coordinates()
        resources = self._build_resources()
        commodities = self._build_commodities()
        node_attrs = self._build_node_attributes()
        node_attrs.update(self._build_station_property_attributes())
        node_attrs.update(self._build_cost_attributes())
        tags = self._build_tags()
        flows = {"trip_flows": self._build_trip_flows()}

        distance_service: DistanceService | None = None
        edges: pd.DataFrame | None = None

        if self._config.build_edges:
            distance_service = DistanceService(
                coordinates=coordinates,
                backend=self._config.distance_backend,
                default_speed_kmh=self._config.default_speed_kmh,
            )
            edges = self._build_edges(nodes=nodes, coordinates=coordinates, distance_service=distance_service)

        self._graph = GraphData(
            nodes=nodes,
            edges=edges,
            resources=resources,
            commodities=commodities,
            coordinates=coordinates,
            node_attributes=node_attrs,
            flows=flows,
            tags=tags,
            distance_service=distance_service,
            inventory_ts=self._source.df_inventory_ts,
            telemetry_ts=self._source.df_telemetry_ts.copy(),
            timestamps=self._source.timestamps,
        )
        # Backward-compatible aliases used in tests and notebooks.
        self._inventory_ts = self._graph.inventory_ts

        self._log.info(
            "load_done",
            nodes=len(self._graph.nodes),
            edges=len(self._graph.edges) if self._graph.edges is not None else 0,
        )

    def get_snapshot(self, date: pd.Timestamp) -> GraphData:
        """Return a ``GraphData`` snapshot for the given *date*."""
        self._log.debug("snapshot", date=str(date))
        return self.graph.get_snapshot(date)

    @property
    def available_dates(self) -> pd.DatetimeIndex:
        return self.graph.available_dates

    @property
    def graph(self) -> GraphData:
        if self._graph is None:
            raise ValueError("Data is not loaded. Call load_data() first.")
        return self._graph

    @property
    def inventory_timeseries(self) -> pd.DataFrame:
        if self.graph.inventory_ts is None:
            return pd.DataFrame()
        return self.graph.inventory_ts

    # ------------------------------------------------------------------
    # Source validation
    # ------------------------------------------------------------------

    def _validate_source(self) -> None:
        StationsSourceSchema.validate(self._source.df_stations)
        DepotsSourceSchema.validate(self._source.df_depots)
        ResourcesSourceSchema.validate(self._source.df_resources)
        self._log.debug("source_validated")

    # ------------------------------------------------------------------
    # Graph construction (static)
    # ------------------------------------------------------------------

    def _build_nodes(self) -> pd.DataFrame:
        stations = self._source.df_stations[["node_id"]].assign(node_type="station")
        depots = self._source.df_depots[["node_id"]].assign(node_type="depot")
        nodes = pd.concat([stations, depots], ignore_index=True)
        return nodes.rename(columns={"node_id": "id"})

    def _build_coordinates(self) -> pd.DataFrame:
        stations = self._source.df_stations[["node_id", "lat", "lon"]]
        depots = self._source.df_depots[["node_id", "lat", "lon"]]
        coords = pd.concat([stations, depots], ignore_index=True)
        return coords.rename(columns={"lat": "latitude", "lon": "longitude"})

    def _build_resources(self) -> pd.DataFrame:
        src = self._source.df_resources.copy()
        resources = pd.DataFrame({
            "id": src["resource_id"].values,
            "resource_type": "vehicle",
            "capacity": src["capacity"].values,
        })
        truck_rates = self._source.df_truck_rates.copy()
        if truck_rates.empty:
            return resources

        merged = resources.merge(
            truck_rates,
            left_on="id",
            right_on="resource_id",
            how="left",
        )
        return merged.drop(columns=["resource_id"])

    @staticmethod
    def _build_commodities() -> pd.DataFrame:
        return pd.DataFrame({
            "id": ["bike"],
            "commodity_type": ["bike"],
        })

    def _build_node_attributes(self) -> dict[str, AttributeTable]:
        stations = self._source.df_stations
        return {
            "inventory_capacity": AttributeTable(
                name="inventory_capacity",
                entity_type="node",
                attribute_class="capacity",
                granularity_keys=["node_id"],
                value_columns=["value"],
                value_types={"value": "int"},
                data=pd.DataFrame({
                    "node_id": stations["node_id"].values,
                    "value": stations["inventory_capacity"].values,
                }),
            ),
        }

    def _build_station_property_attributes(self) -> dict[str, AttributeTable]:
        stations = self._source.df_stations
        required = {"node_id", "name", "short_name"}
        if not required.issubset(stations.columns):
            return {}

        return {
            "station_info": AttributeTable(
                name="station_info",
                entity_type="node",
                attribute_class="property",
                granularity_keys=["node_id"],
                value_columns=["name", "short_name"],
                value_types={"name": "str", "short_name": "str"},
                data=stations[["node_id", "name", "short_name"]].copy(),
            ),
        }

    def _build_cost_attributes(self) -> dict[str, AttributeTable]:
        costs = self._source.df_station_costs.copy()
        if costs.empty:
            return {}

        fixed_data = costs.rename(columns={"station_id": "node_id"})[
            ["node_id", "fixed_cost_per_visit"]
        ].rename(columns={"fixed_cost_per_visit": "value"})
        variable_data = costs.rename(columns={"station_id": "node_id"})[
            ["node_id", "cost_per_bike_moved"]
        ].rename(columns={"cost_per_bike_moved": "value"})

        return {
            "station_fixed_cost": AttributeTable(
                name="station_fixed_cost",
                entity_type="node",
                attribute_class="cost",
                granularity_keys=["node_id"],
                value_columns=["value"],
                value_types={"value": "float"},
                data=fixed_data,
            ),
            "station_variable_cost": AttributeTable(
                name="station_variable_cost",
                entity_type="node",
                attribute_class="cost",
                granularity_keys=["node_id"],
                value_columns=["value"],
                value_types={"value": "float"},
                data=variable_data,
            ),
        }

    def _build_tags(self) -> pd.DataFrame | None:
        stations = self._source.df_stations
        if "region_id" not in stations.columns:
            return None

        return pd.DataFrame({
            "entity_type": "node",
            "entity_id": stations["node_id"].values,
            "key": "region",
            "value": stations["region_id"].astype(str).values,
        })

    def _build_trip_flows(self) -> FlowsTable:
        trips = self._source.df_trips.copy()
        if trips.empty:
            flow_data = pd.DataFrame(columns=["source_id", "target_id", "period", "value"])
        else:
            flow_data = (
                trips.assign(
                    source_id=trips["start_station_id"],
                    target_id=trips["end_station_id"],
                    period=pd.to_datetime(trips["started_at"]).dt.floor("h"),
                )
                .groupby(["source_id", "target_id", "period"], as_index=False)
                .size()
                .rename(columns={"size": "value"})
            )

        return FlowsTable(
            name="trip_flows",
            granularity_keys=["source_id", "target_id", "period"],
            value_column="value",
            value_type="int",
            data=flow_data,
            description="Aggregated hourly trip counts between stations",
        )

    def _build_edges(
        self,
        nodes: pd.DataFrame,
        coordinates: pd.DataFrame,
        distance_service: DistanceService,
    ) -> pd.DataFrame:
        temp_graph = GraphData(nodes=nodes, coordinates=coordinates)
        builder = EdgeBuilder(
            graph=temp_graph,
            distance_service=distance_service,
        )
        return builder.build_complete_graph(node_ids=list(nodes["id"]))
