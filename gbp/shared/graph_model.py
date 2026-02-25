"""Graph data model — unified structure for all modules.

Defines the GraphData container and type enumerations.
Adapted from the pandera-based Silver Layer model (old/graph_model.py)
but kept lightweight (no pandera dependency).
"""

from enum import Enum

import numpy as np
import pandas as pd


# =============================================================================
# Type Enumerations
# =============================================================================


class NodeType(str, Enum):
    STATION = "station"
    DEPOT = "depot"


class EdgeType(str, Enum):
    USAGE = "usage"
    TRANSFER = "transfer"


class ResourceType(str, Enum):
    VEHICLE = "vehicle"


class CommodityType(str, Enum):
    BIKE = "bike"


# =============================================================================
# GraphData Container
# =============================================================================


class GraphData:
    """Complete graph dataset.

    DataFrames follow these schemas (column order is indicative):

        nodes:       [node_id, node_type, node_name, lat, lon]
        edges:       [source_id, target_id, edge_type, distance]
        resources:   [resource_id, resource_type, resource_capacity]
        commodities: [commodity_id, commodity_name, commodity_type]
        demands:     [node_id, commodity_type, quantity]
        inventory:   [node_id, commodity_type, commodity_quantity, inventory_capacity]

    Only *nodes* is required; every other table is optional.
    """

    def __init__(
        self,
        nodes: pd.DataFrame,
        edges: pd.DataFrame | None = None,
        resources: pd.DataFrame | None = None,
        commodities: pd.DataFrame | None = None,
        demands: pd.DataFrame | None = None,
        inventory: pd.DataFrame | None = None,
    ) -> None:
        if len(nodes) == 0:
            raise ValueError("nodes DataFrame must have at least 1 row")

        self._nodes = nodes
        self._edges = edges
        self._resources = resources
        self._commodities = commodities
        self._demands = demands
        self._inventory = inventory

    # ---- properties --------------------------------------------------------

    @property
    def nodes(self) -> pd.DataFrame:
        return self._nodes

    @property
    def edges(self) -> pd.DataFrame | None:
        return self._edges

    @property
    def resources(self) -> pd.DataFrame | None:
        return self._resources

    @property
    def commodities(self) -> pd.DataFrame | None:
        return self._commodities

    @property
    def demands(self) -> pd.DataFrame | None:
        return self._demands

    @property
    def inventory(self) -> pd.DataFrame | None:
        return self._inventory

    @property
    def node_ids(self) -> set[str]:
        return set(self._nodes["node_id"])

    # ---- helpers -----------------------------------------------------------

    def filter_nodes_by_type(self, node_type: str | NodeType) -> pd.DataFrame:
        """Filter nodes by *node_type* (accepts enum or plain string)."""
        type_value = node_type.value if isinstance(node_type, NodeType) else node_type
        return self._nodes[self._nodes["node_type"] == type_value].copy()

    def filter_edges_by_type(self, edge_type: str | EdgeType) -> pd.DataFrame:
        type_value = edge_type.value if isinstance(edge_type, EdgeType) else edge_type
        if self._edges is None:
            return pd.DataFrame(columns=["source_id", "target_id", "edge_type", "distance"])
        return self._edges[self._edges["edge_type"] == type_value].copy()

    def to_distance_matrix(self) -> np.ndarray:
        """NxN distance matrix (np.inf for missing edges, 0 on diagonal)."""
        n = len(self._nodes)
        node_id_to_idx = pd.Series(range(n), index=self._nodes["node_id"])

        matrix = np.full((n, n), np.inf)
        np.fill_diagonal(matrix, 0)

        if self._edges is not None and "distance" in self._edges.columns:
            src = node_id_to_idx[self._edges["source_id"]].values
            tgt = node_id_to_idx[self._edges["target_id"]].values
            matrix[src, tgt] = self._edges["distance"].values

        return matrix

    def get_node_coords(self) -> np.ndarray:
        """Nx2 array of [lat, lon]."""
        return self._nodes[["lat", "lon"]].to_numpy()

    def __repr__(self) -> str:
        parts = [f"nodes={len(self._nodes)}"]
        for name, df in [
            ("edges", self._edges),
            ("resources", self._resources),
            ("commodities", self._commodities),
            ("demands", self._demands),
            ("inventory", self._inventory),
        ]:
            parts.append(f"{name}={len(df) if df is not None else 0}")
        return f"GraphData({', '.join(parts)})"
