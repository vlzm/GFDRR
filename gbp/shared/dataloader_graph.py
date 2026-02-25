import numpy as np
import pandas as pd
from scipy.spatial.distance import cdist

from .graph_model import (
    CommodityType,
    EdgeType,
    GraphData,
    NodeType,
    ResourceType,
)


class DataLoaderGraph:
    """Convert raw source data into a temporal GraphData representation.

    Static parts (nodes, edges, resources, commodities) are built once.
    Inventory varies over time; call ``get_snapshot(date)`` to obtain a
    complete ``GraphData`` for a specific moment.

    Usage::

        graph_loader = DataLoaderGraph(mock_source, config)
        graph_loader.load_data()
        snapshot = graph_loader.get_snapshot(pd.Timestamp('2025-01-03 12:00'))
        print(snapshot)
    """

    def __init__(self, dataloader_source, config: dict):
        self.config = config
        self._source = dataloader_source

    def load_data(self) -> None:
        self._source.load_data()

        self._nodes = self._build_nodes()
        self._edges = self._build_edges()
        self._resources = self._build_resources()
        self._commodities = self._build_commodities()

        self._inventory_ts = self._source.df_inventory_ts
        self._timestamps = self._source.timestamps

        self._node_capacities = (
            self._source.df_stations.set_index('node_id')['inventory_capacity']
        )

    # ------------------------------------------------------------------
    # Graph construction (static parts)
    # ------------------------------------------------------------------

    def _build_nodes(self) -> pd.DataFrame:
        stations = self._source.df_stations
        depots = self._source.df_depots

        station_nodes = pd.DataFrame({
            "node_id": stations["node_id"].values,
            "node_type": NodeType.STATION.value,
            "node_name": stations["node_id"].str.replace("_", " ").str.title().values,
            "lat": stations["lat"].values,
            "lon": stations["lon"].values,
        })

        depot_nodes = pd.DataFrame({
            "node_id": depots["node_id"].values,
            "node_type": NodeType.DEPOT.value,
            "node_name": depots["node_id"].str.replace("_", " ").str.title().values,
            "lat": depots["lat"].values,
            "lon": depots["lon"].values,
        })

        return pd.concat([station_nodes, depot_nodes], ignore_index=True)

    def _build_edges(self) -> pd.DataFrame:
        """Fully-connected edges with approximate haversine distances (km)."""
        coords = self._nodes[["lat", "lon"]].values
        ids = self._nodes["node_id"].values
        n = len(ids)

        mean_lat = np.radians(coords[:, 0].mean())
        scaled = coords.copy()
        scaled[:, 1] *= np.cos(mean_lat)
        dist_km = cdist(scaled, scaled, metric='euclidean') * 111.0

        src_idx, tgt_idx = np.where(~np.eye(n, dtype=bool))

        return pd.DataFrame({
            "source_id": ids[src_idx],
            "target_id": ids[tgt_idx],
            "edge_type": EdgeType.TRANSFER.value,
            "distance": np.round(dist_km[src_idx, tgt_idx], 3),
        })

    def _build_resources(self) -> pd.DataFrame:
        src = self._source.df_resources
        return pd.DataFrame({
            "resource_id": src["resource_id"].values,
            "resource_type": ResourceType.VEHICLE.value,
            "resource_capacity": src["capacity"].values,
        })

    @staticmethod
    def _build_commodities() -> pd.DataFrame:
        return pd.DataFrame({
            "commodity_id": ["bike"],
            "commodity_name": ["Bike"],
            "commodity_type": [CommodityType.BIKE.value],
        })

    # ------------------------------------------------------------------
    # Temporal snapshots
    # ------------------------------------------------------------------

    def _build_inventory_snapshot(self, date: pd.Timestamp) -> pd.DataFrame:
        idx = self._inventory_ts.index.get_indexer([date], method='nearest')[0]
        ts = self._inventory_ts.index[idx]
        quantities = self._inventory_ts.loc[ts]

        node_ids = quantities.index.tolist()
        return pd.DataFrame({
            "node_id": node_ids,
            "commodity_type": CommodityType.BIKE.value,
            "commodity_quantity": quantities.values.astype(int),
            "inventory_capacity": self._node_capacities.loc[node_ids].values,
        })

    def get_snapshot(self, date: pd.Timestamp) -> GraphData:
        """Return a complete GraphData for the given *date*."""
        return GraphData(
            nodes=self._nodes.copy(),
            edges=self._edges.copy(),
            resources=self._resources.copy(),
            commodities=self._commodities.copy(),
            inventory=self._build_inventory_snapshot(date),
        )

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @property
    def available_dates(self) -> pd.DatetimeIndex:
        return self._timestamps

    @property
    def inventory_timeseries(self) -> pd.DataFrame:
        return self._inventory_ts
