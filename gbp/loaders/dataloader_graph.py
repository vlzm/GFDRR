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

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_data(self) -> None:
        """Load source data and build all static graph parts."""
        self._log.info("load_start")

        self._source.load_data()
        self._validate_source()

        self._nodes = self._build_nodes()
        self._coordinates = self._build_coordinates()
        self._resources = self._build_resources()
        self._commodities = self._build_commodities()
        self._node_attrs = self._build_node_attributes()

        self._inventory_ts = self._source.df_inventory_ts
        self._timestamps = self._source.timestamps

        self._distance_service: DistanceService | None = None
        self._edges: pd.DataFrame | None = None

        if self._config.build_edges:
            self._distance_service = DistanceService(
                coordinates=self._coordinates,
                backend=self._config.distance_backend,
                default_speed_kmh=self._config.default_speed_kmh,
            )
            self._edges = self._build_edges()

        self._log.info(
            "load_done",
            nodes=len(self._nodes),
            edges=len(self._edges) if self._edges is not None else 0,
        )

    def get_snapshot(self, date: pd.Timestamp) -> GraphData:
        """Return a ``GraphData`` snapshot for the given *date*."""
        self._log.debug("snapshot", date=str(date))
        return GraphData(
            nodes=self._nodes.copy(),
            edges=self._edges.copy() if self._edges is not None else None,
            resources=self._resources.copy(),
            commodities=self._commodities.copy(),
            coordinates=self._coordinates.copy(),
            node_attributes=dict(self._node_attrs),
            inventory=self._build_inventory_snapshot(date),
            distance_service=self._distance_service,
        )

    @property
    def available_dates(self) -> pd.DatetimeIndex:
        return self._timestamps

    @property
    def inventory_timeseries(self) -> pd.DataFrame:
        return self._inventory_ts

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
        src = self._source.df_resources
        return pd.DataFrame({
            "id": src["resource_id"].values,
            "resource_type": "vehicle",
            "capacity": src["capacity"].values,
        })

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

    def _build_edges(self) -> pd.DataFrame:
        temp_graph = GraphData(nodes=self._nodes, coordinates=self._coordinates)
        builder = EdgeBuilder(
            graph=temp_graph,
            distance_service=self._distance_service,
        )
        return builder.build_complete_graph(node_ids=list(self._nodes["id"]))

    # ------------------------------------------------------------------
    # Temporal snapshots
    # ------------------------------------------------------------------

    def _build_inventory_snapshot(self, date: pd.Timestamp) -> pd.DataFrame:
        idx = self._inventory_ts.index.get_indexer([date], method="nearest")[0]
        ts = self._inventory_ts.index[idx]
        quantities = self._inventory_ts.loc[ts]
        return pd.DataFrame({
            "node_id": quantities.index.tolist(),
            "commodity_id": "bike",
            "quantity": quantities.values.astype(int),
        })
