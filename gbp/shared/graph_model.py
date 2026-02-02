"""DataFrame-based models for Silver Layer Graph representation.

These models define the universal graph structure used across all modules.
All data crossing module boundaries MUST use these validated DataFrame models.
Uses pandera for declarative DataFrame validation.
"""

from enum import Enum

import numpy as np
import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame, Series


# =============================================================================
# Type Enumerations
# =============================================================================


class NodeType(str, Enum):
    """Enumeration of supported node types."""

    DEPOT = "depot"


class EdgeType(str, Enum):
    """Enumeration of supported edge types."""

    USAGE = "usage"
    TRANSFER = "transfer"


class ResourceType(str, Enum):
    """Enumeration of supported resource types."""

    VEHICLE = "vehicle"


class CommodityType(str, Enum):
    """Enumeration of supported commodity types."""

    SCOOTER = "scooter"


# =============================================================================
# Pandera Schema Definitions
# =============================================================================


class NodesSchema(pa.DataFrameModel):
    """Schema for nodes DataFrame.

    Columns:
        id: Unique node identifier.
        node_type: Type classification (station, depot, customer).
        node_name: Human-readable name of the node.
        latitude: Geographic latitude coordinate.
        longitude: Geographic longitude coordinate.
    """

    id: Series[str] = pa.Field(unique=True, str_length={"min_value": 1})
    node_type: Series[str] = pa.Field(isin=[e.value for e in NodeType])
    node_name: Series[str] = pa.Field(str_length={"min_value": 1})
    latitude: Series[float] = pa.Field(ge=-90, le=90)
    longitude: Series[float] = pa.Field(ge=-180, le=180)

    class Config:
        """Pandera configuration."""

        strict = True
        coerce = True


class EdgesSchema(pa.DataFrameModel):
    """Schema for edges DataFrame.

    Columns:
        source_id: Origin node identifier.
        target_id: Destination node identifier.
        edge_type: Type of connection (usage, transfer).
        distance: Optional distance/cost of the edge.
    """

    source_id: Series[str] = pa.Field(str_length={"min_value": 1})
    target_id: Series[str] = pa.Field(str_length={"min_value": 1})
    edge_type: Series[str] = pa.Field(isin=[e.value for e in EdgeType])
    distance: Series[float] = pa.Field(ge=0, nullable=True, default=None)

    class Config:
        """Pandera configuration."""

        strict = False  # Allow additional columns
        coerce = True


class ResourcesSchema(pa.DataFrameModel):
    """Schema for resources DataFrame.

    Columns:
        resource_id: Unique resource identifier.
        resource_type: Type of resource (vehicle).
        capacity: Maximum capacity of the resource.
    """

    resource_id: Series[str] = pa.Field(unique=True, str_length={"min_value": 1})
    resource_type: Series[str] = pa.Field(isin=[e.value for e in ResourceType])
    capacity: Series[int] = pa.Field(ge=0, nullable=True, default=None)

    class Config:
        """Pandera configuration."""

        strict = False
        coerce = True


class CommoditiesSchema(pa.DataFrameModel):
    """Schema for commodities DataFrame.

    Columns:
        commodity_id: Unique commodity identifier.
        commodity_name: Human-readable name.
        commodity_type: Type of commodity (scooter).
    """

    commodity_id: Series[str] = pa.Field(unique=True, str_length={"min_value": 1})
    commodity_name: Series[str] = pa.Field(str_length={"min_value": 1})
    commodity_type: Series[str] = pa.Field(isin=[e.value for e in CommodityType])

    class Config:
        """Pandera configuration."""

        strict = True
        coerce = True


class DemandsSchema(pa.DataFrameModel):
    """Schema for demands DataFrame.

    Columns:
        node_id: Node where demand occurs.
        commodity_type: Type of commodity demanded.
        quantity: Amount demanded (non-negative).
    """

    node_id: Series[str] = pa.Field(str_length={"min_value": 1})
    commodity_type: Series[str] = pa.Field(isin=[e.value for e in CommodityType])
    quantity: Series[int] = pa.Field(ge=0)

    class Config:
        """Pandera configuration."""

        strict = True
        coerce = True


class InventorySchema(pa.DataFrameModel):
    """Schema for inventory DataFrame.

    Columns:
        node_id: Node where inventory is stored.
        commodity_type: Type of commodity.
        quantity: Amount available (non-negative).
    """

    node_id: Series[str] = pa.Field(str_length={"min_value": 1})
    commodity_type: Series[str] = pa.Field(isin=[e.value for e in CommodityType])
    quantity: Series[int] = pa.Field(ge=0)

    class Config:
        """Pandera configuration."""

        strict = True
        coerce = True


# =============================================================================
# Type Aliases for Validated DataFrames
# =============================================================================

NodesDF = DataFrame[NodesSchema]
EdgesDF = DataFrame[EdgesSchema]
ResourcesDF = DataFrame[ResourcesSchema]
CommoditiesDF = DataFrame[CommoditiesSchema]
DemandsDF = DataFrame[DemandsSchema]
InventoryDF = DataFrame[InventorySchema]


# =============================================================================
# GraphData Container
# =============================================================================


class GraphData:
    """Complete graph dataset (Silver Layer).

    This is the primary data structure passed between modules.
    All DataFrames are validated on construction using pandera schemas.

    Attributes:
        nodes: DataFrame with columns [id, node_type, node_name, latitude, longitude].
        edges: DataFrame with columns [source_id, target_id, edge_type, distance?].
        demands: Optional DataFrame with columns [node_id, commodity_type, quantity].
        inventory: Optional DataFrame with columns [node_id, commodity_type, quantity].

    Example:
        >>> nodes = pd.DataFrame({
        ...     "id": ["n1", "n2"],
        ...     "node_type": ["station", "depot"],
        ...     "node_name": ["Station A", "Depot B"],
        ...     "latitude": [55.0, 56.0],
        ...     "longitude": [37.0, 38.0],
        ... })
        >>> edges = pd.DataFrame({
        ...     "source_id": ["n1"],
        ...     "target_id": ["n2"],
        ...     "edge_type": ["usage"],
        ... })
        >>> graph = GraphData(nodes=nodes, edges=edges)
    """

    def __init__(
        self,
        nodes: pd.DataFrame,
        edges: pd.DataFrame | None = None,
        demands: pd.DataFrame | None = None,
        inventory: pd.DataFrame | None = None,
        *,
        validate: bool = True,
    ) -> None:
        """Initialize GraphData with validated DataFrames.

        Args:
            nodes: Nodes DataFrame (required, must have at least 1 row).
            edges: Edges DataFrame (optional).
            demands: Demands DataFrame (optional).
            inventory: Inventory DataFrame (optional).
            validate: Whether to validate DataFrames (default True).

        Raises:
            pa.errors.SchemaError: If validation fails.
            ValueError: If nodes is empty or referential integrity is violated.
        """
        if len(nodes) == 0:
            raise ValueError("nodes DataFrame must have at least 1 row")

        if validate:
            self._nodes = NodesSchema.validate(nodes)
            self._edges = (
                EdgesSchema.validate(edges) if edges is not None and len(edges) > 0 else None
            )
            self._demands = (
                DemandsSchema.validate(demands)
                if demands is not None and len(demands) > 0
                else None
            )
            self._inventory = (
                InventorySchema.validate(inventory)
                if inventory is not None and len(inventory) > 0
                else None
            )
            self._validate_referential_integrity()
        else:
            self._nodes = nodes
            self._edges = edges if edges is not None and len(edges) > 0 else None
            self._demands = demands if demands is not None and len(demands) > 0 else None
            self._inventory = inventory if inventory is not None and len(inventory) > 0 else None

    def _validate_referential_integrity(self) -> None:
        """Validate that all foreign keys reference existing nodes."""
        node_ids = set(self._nodes["id"])

        if self._edges is not None:
            invalid_sources = set(self._edges["source_id"]) - node_ids
            if invalid_sources:
                raise ValueError(f"edges.source_id references unknown nodes: {invalid_sources}")
            invalid_targets = set(self._edges["target_id"]) - node_ids
            if invalid_targets:
                raise ValueError(f"edges.target_id references unknown nodes: {invalid_targets}")

        if self._demands is not None:
            invalid_nodes = set(self._demands["node_id"]) - node_ids
            if invalid_nodes:
                raise ValueError(f"demands.node_id references unknown nodes: {invalid_nodes}")

        if self._inventory is not None:
            invalid_nodes = set(self._inventory["node_id"]) - node_ids
            if invalid_nodes:
                raise ValueError(f"inventory.node_id references unknown nodes: {invalid_nodes}")

    @property
    def nodes(self) -> pd.DataFrame:
        """Get nodes DataFrame."""
        return self._nodes

    @property
    def edges(self) -> pd.DataFrame:
        """Get edges DataFrame (empty DataFrame if None)."""
        if self._edges is None:
            return pd.DataFrame(columns=["source_id", "target_id", "edge_type"])
        return self._edges

    @property
    def demands(self) -> pd.DataFrame | None:
        """Get demands DataFrame."""
        return self._demands

    @property
    def inventory(self) -> pd.DataFrame | None:
        """Get inventory DataFrame."""
        return self._inventory

    @property
    def node_ids(self) -> set[str]:
        """Get set of all node IDs."""
        return set(self._nodes["id"])

    def to_distance_matrix(self) -> np.ndarray:
        """Convert edges to distance matrix (Gold Layer transformation).

        Returns:
            NxN NumPy array where matrix[i][j] = distance from node i to j.
            Diagonal is 0, missing edges are np.inf.

        Note:
            Uses vectorized operations. No for-loops.
        """
        n = len(self._nodes)
        node_id_to_idx = pd.Series(range(n), index=self._nodes["id"])

        matrix = np.full((n, n), np.inf)
        np.fill_diagonal(matrix, 0)

        if self._edges is not None and "distance" in self._edges.columns:
            source_idx = node_id_to_idx[self._edges["source_id"]].values
            target_idx = node_id_to_idx[self._edges["target_id"]].values
            matrix[source_idx, target_idx] = self._edges["distance"].values

        return matrix

    def get_node_coords(self) -> np.ndarray:
        """Get node coordinates as NumPy array.

        Returns:
            Nx2 array with [latitude, longitude] for each node.
        """
        return self._nodes[["latitude", "longitude"]].to_numpy()

    def filter_nodes_by_type(self, node_type: NodeType) -> pd.DataFrame:
        """Filter nodes by type.

        Args:
            node_type: Type to filter by.

        Returns:
            Filtered DataFrame (copy).
        """
        return self._nodes[self._nodes["node_type"] == node_type.value].copy()

    def filter_edges_by_type(self, edge_type: EdgeType) -> pd.DataFrame:
        """Filter edges by type.

        Args:
            edge_type: Type to filter by.

        Returns:
            Filtered DataFrame (copy).
        """
        if self._edges is None:
            return pd.DataFrame(columns=["source_id", "target_id", "edge_type"])
        return self._edges[self._edges["edge_type"] == edge_type.value].copy()

    def __repr__(self) -> str:
        """Return string representation."""
        return (
            f"GraphData(nodes={len(self._nodes)}, "
            f"edges={len(self._edges) if self._edges is not None else 0}, "
            f"demands={len(self._demands) if self._demands is not None else 0}, "
            f"inventory={len(self._inventory) if self._inventory is not None else 0})"
        )
