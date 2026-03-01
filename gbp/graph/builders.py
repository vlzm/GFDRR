"""Dynamic builders for edges and distances.

This module provides services that compute data on-demand:
- DistanceService: Computes distances between nodes
- EdgeBuilder: Builds edges between nodes with distances and attributes

These are separate from GraphData because they compute, not store.
"""

from dataclasses import dataclass
from typing import Literal
import math

import numpy as np
import pandas as pd

from gbp.graph.core import GraphData


# =============================================================================
# Distance Service
# =============================================================================


DistanceBackend = Literal["haversine", "euclidean"]


@dataclass
class DistanceService:
    """Computes distances and durations between nodes.
    
    Supports multiple backends:
    - haversine: Great-circle distance (for lat/lon coordinates)
    - euclidean: Straight-line distance
    
    Future backends could include OSRM, Google Maps API, etc.
    
    Attributes:
        coordinates: DataFrame with node_id, latitude, longitude.
        backend: Distance calculation method.
        default_speed_kmh: Default speed for duration estimation (km/h).
    
    Example:
        >>> service = DistanceService(
        ...     coordinates=graph.coordinates,
        ...     backend="haversine"
        ... )
        >>> dist = service.get_distance("depot_1", "zone_001")
        >>> matrix = service.get_matrix(["depot_1", "depot_2"], ["zone_001", "zone_002"])
    """
    
    coordinates: pd.DataFrame
    backend: DistanceBackend = "haversine"
    default_speed_kmh: float = 50.0
    
    def __post_init__(self) -> None:
        """Build coordinate lookup."""
        self._coord_lookup: dict[str, tuple[float, float]] = {}
        for _, row in self.coordinates.iterrows():
            self._coord_lookup[row["node_id"]] = (row["latitude"], row["longitude"])
    
    def get_distance(
        self,
        source_id: str,
        target_id: str
    ) -> float:
        """Get distance between two nodes.
        
        Args:
            source_id: Source node ID.
            target_id: Target node ID.
        
        Returns:
            Distance in kilometers.
        
        Raises:
            KeyError: If node not found in coordinates.
        """
        if source_id not in self._coord_lookup:
            raise KeyError(f"Node '{source_id}' not found in coordinates")
        if target_id not in self._coord_lookup:
            raise KeyError(f"Node '{target_id}' not found in coordinates")
        
        lat1, lon1 = self._coord_lookup[source_id]
        lat2, lon2 = self._coord_lookup[target_id]
        
        if self.backend == "haversine":
            return self._haversine(lat1, lon1, lat2, lon2)
        elif self.backend == "euclidean":
            return self._euclidean(lat1, lon1, lat2, lon2)
        else:
            raise ValueError(f"Unknown backend: {self.backend}")
    
    def get_duration(
        self,
        source_id: str,
        target_id: str,
        speed_kmh: float | None = None
    ) -> float:
        """Get estimated travel duration between two nodes.
        
        Args:
            source_id: Source node ID.
            target_id: Target node ID.
            speed_kmh: Travel speed in km/h. Uses default if None.
        
        Returns:
            Duration in hours.
        """
        distance = self.get_distance(source_id, target_id)
        speed = speed_kmh or self.default_speed_kmh
        return distance / speed
    
    def get_matrix(
        self,
        source_ids: list[str],
        target_ids: list[str],
        include_duration: bool = True,
        speed_kmh: float | None = None
    ) -> pd.DataFrame:
        """Get distance matrix between sets of nodes.
        
        Args:
            source_ids: List of source node IDs.
            target_ids: List of target node IDs.
            include_duration: Whether to include duration column.
            speed_kmh: Travel speed for duration calculation.
        
        Returns:
            DataFrame with columns: source_id, target_id, distance_km, [duration_h].
        """
        rows = []
        speed = speed_kmh or self.default_speed_kmh
        
        for source_id in source_ids:
            for target_id in target_ids:
                if source_id == target_id:
                    continue
                
                distance = self.get_distance(source_id, target_id)
                row = {
                    "source_id": source_id,
                    "target_id": target_id,
                    "distance_km": distance,
                }
                
                if include_duration:
                    row["duration_h"] = distance / speed
                
                rows.append(row)
        
        return pd.DataFrame(rows)
    
    def get_matrix_numpy(
        self,
        node_ids: list[str]
    ) -> tuple[np.ndarray, dict[str, int]]:
        """Get distance matrix as NumPy array.
        
        Args:
            node_ids: List of node IDs (defines row/column order).
        
        Returns:
            Tuple of (NxN distance matrix, node_id to index mapping).
        """
        n = len(node_ids)
        node_to_idx = {node_id: i for i, node_id in enumerate(node_ids)}
        
        matrix = np.zeros((n, n))
        
        for i, source_id in enumerate(node_ids):
            for j, target_id in enumerate(node_ids):
                if i != j:
                    matrix[i, j] = self.get_distance(source_id, target_id)
        
        return matrix, node_to_idx
    
    # =========================================================================
    # Distance Calculations
    # =========================================================================
    
    @staticmethod
    def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate great-circle distance using Haversine formula.
        
        Args:
            lat1, lon1: First point coordinates (degrees).
            lat2, lon2: Second point coordinates (degrees).
        
        Returns:
            Distance in kilometers.
        """
        R = 6371  # Earth radius in km
        
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)
        
        a = (
            math.sin(delta_lat / 2) ** 2 +
            math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        return R * c
    
    @staticmethod
    def _euclidean(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate Euclidean distance (approximate, for small areas).
        
        Converts lat/lon to approximate km using simple scaling.
        Only accurate for small regions!
        
        Args:
            lat1, lon1: First point coordinates (degrees).
            lat2, lon2: Second point coordinates (degrees).
        
        Returns:
            Approximate distance in kilometers.
        """
        # Approximate conversion factors
        lat_km = 111.0  # km per degree latitude
        lon_km = 111.0 * math.cos(math.radians((lat1 + lat2) / 2))  # varies by latitude
        
        dx = (lon2 - lon1) * lon_km
        dy = (lat2 - lat1) * lat_km
        
        return math.sqrt(dx ** 2 + dy ** 2)


# =============================================================================
# Edge Builder
# =============================================================================


@dataclass
class EdgeBuilder:
    """Builds edges dynamically based on node types and filters.
    
    Edges are not stored in GraphData — they are computed on demand.
    This avoids storing potentially huge N×M edge combinations.
    
    Attributes:
        graph: GraphData instance to build edges from.
        distance_service: Optional DistanceService for distance calculations.
    
    Example:
        >>> builder = EdgeBuilder(graph, distance_service)
        >>> 
        >>> # Build all edges from depots to zones
        >>> edges = builder.build(
        ...     source_types=["depot"],
        ...     target_types=["zone"]
        ... )
        >>> 
        >>> # Build edges with attributes
        >>> edges = builder.build_with_attrs(
        ...     source_types=["depot"],
        ...     target_types=["zone"],
        ...     attr_names=["transport_rate", "trip_cost"],
        ...     commodity_id="bulk"
        ... )
    """
    
    graph: GraphData
    distance_service: DistanceService | None = None
    
    def build(
        self,
        source_ids: list[str] | None = None,
        target_ids: list[str] | None = None,
        source_types: list[str] | None = None,
        target_types: list[str] | None = None,
        include_distances: bool = True,
        include_self_loops: bool = False,
        bidirectional: bool = False
    ) -> pd.DataFrame:
        """Build edges between nodes.
        
        Args:
            source_ids: Explicit list of source node IDs.
            target_ids: Explicit list of target node IDs.
            source_types: Node types for sources (used if source_ids is None).
            target_types: Node types for targets (used if target_ids is None).
            include_distances: Whether to compute and include distances.
            include_self_loops: Whether to include edges where source=target.
            bidirectional: If True, include both A→B and B→A edges.
        
        Returns:
            DataFrame with columns: source_id, target_id, [distance_km, duration_h].
        """
        # Resolve source_ids
        if source_ids is None:
            if source_types is None:
                source_ids = list(self.graph.nodes["id"])
            else:
                mask = self.graph.nodes["node_type"].isin(source_types)
                source_ids = list(self.graph.nodes[mask]["id"])
        
        # Resolve target_ids
        if target_ids is None:
            if target_types is None:
                target_ids = list(self.graph.nodes["id"])
            else:
                mask = self.graph.nodes["node_type"].isin(target_types)
                target_ids = list(self.graph.nodes[mask]["id"])
        
        # Build edge pairs
        pairs = []
        seen = set()
        
        for source_id in source_ids:
            for target_id in target_ids:
                if not include_self_loops and source_id == target_id:
                    continue
                
                # Avoid duplicates if bidirectional
                if not bidirectional:
                    pairs.append((source_id, target_id))
                else:
                    # Add both directions, but avoid exact duplicates
                    if (source_id, target_id) not in seen:
                        pairs.append((source_id, target_id))
                        seen.add((source_id, target_id))
                    if (target_id, source_id) not in seen:
                        pairs.append((target_id, source_id))
                        seen.add((target_id, source_id))
        
        # Build DataFrame
        if not pairs:
            columns = ["source_id", "target_id"]
            if include_distances:
                columns.extend(["distance_km", "duration_h"])
            return pd.DataFrame(columns=columns)
        
        edges = pd.DataFrame(pairs, columns=["source_id", "target_id"])
        
        # Add distances if requested
        if include_distances and self.distance_service is not None:
            distances = []
            durations = []
            
            for _, row in edges.iterrows():
                try:
                    dist = self.distance_service.get_distance(
                        row["source_id"], row["target_id"]
                    )
                    dur = self.distance_service.get_duration(
                        row["source_id"], row["target_id"]
                    )
                except KeyError:
                    dist = None
                    dur = None
                
                distances.append(dist)
                durations.append(dur)
            
            edges["distance_km"] = distances
            edges["duration_h"] = durations
        
        return edges
    
    def build_with_attrs(
        self,
        source_ids: list[str] | None = None,
        target_ids: list[str] | None = None,
        source_types: list[str] | None = None,
        target_types: list[str] | None = None,
        attr_names: list[str] | None = None,
        include_distances: bool = True,
        include_self_loops: bool = False,
        **filters
    ) -> pd.DataFrame:
        """Build edges with attributes merged.
        
        Convenience method that combines build() with attribute merging.
        
        Args:
            source_ids: Explicit list of source node IDs.
            target_ids: Explicit list of target node IDs.
            source_types: Node types for sources.
            target_types: Node types for targets.
            attr_names: Edge attribute names to merge.
            include_distances: Whether to include distance calculations.
            include_self_loops: Whether to include self-loops.
            **filters: Filters for attributes (e.g., commodity_id="bulk").
        
        Returns:
            DataFrame with edges and merged attributes.
        """
        # Build base edges
        edges = self.build(
            source_ids=source_ids,
            target_ids=target_ids,
            source_types=source_types,
            target_types=target_types,
            include_distances=include_distances,
            include_self_loops=include_self_loops
        )
        
        if attr_names is None:
            return edges
        
        # Merge attributes
        for attr_name in attr_names:
            if attr_name not in self.graph.edge_attributes:
                raise KeyError(f"Edge attribute '{attr_name}' not found")
            
            attr = self.graph.edge_attributes[attr_name]
            attr_df = attr.data.copy()
            
            # Apply filters
            for key, value in filters.items():
                if key in attr_df.columns:
                    attr_df = attr_df[attr_df[key] == value]
            
            # Rename value columns to avoid conflicts
            value_rename = {}
            for col in attr.value_columns:
                if col in edges.columns:
                    value_rename[col] = f"{attr_name}_{col}"
            if value_rename:
                attr_df = attr_df.rename(columns=value_rename)
            
            # Merge on source_id, target_id
            edges = edges.merge(
                attr_df,
                on=["source_id", "target_id"],
                how="left"
            )
        
        return edges
    
    def build_complete_graph(
        self,
        node_ids: list[str],
        include_distances: bool = True
    ) -> pd.DataFrame:
        """Build complete graph (all nodes connected to all nodes).
        
        Args:
            node_ids: List of node IDs to include.
            include_distances: Whether to compute distances.
        
        Returns:
            DataFrame with all edges (no self-loops).
        """
        return self.build(
            source_ids=node_ids,
            target_ids=node_ids,
            include_distances=include_distances,
            include_self_loops=False,
            bidirectional=True
        )
    
    def build_from_flows(
        self,
        flow_name: str,
        include_distances: bool = True,
        **filters
    ) -> pd.DataFrame:
        """Build edges based on existing flow data.
        
        Creates edges only where flows exist (sparse graph).
        
        Args:
            flow_name: Name of the flows table.
            include_distances: Whether to compute distances.
            **filters: Filters for flows.
        
        Returns:
            DataFrame with edges derived from flows.
        """
        if flow_name not in self.graph.flows:
            raise KeyError(f"Flows '{flow_name}' not found")
        
        flows_df = self.graph.flows[flow_name].data.copy()
        
        # Apply filters
        for key, value in filters.items():
            if key in flows_df.columns:
                flows_df = flows_df[flows_df[key] == value]
        
        # Get unique edges
        edges = flows_df[["source_id", "target_id"]].drop_duplicates()
        
        # Add distances if requested
        if include_distances and self.distance_service is not None:
            distances = []
            durations = []
            
            for _, row in edges.iterrows():
                try:
                    dist = self.distance_service.get_distance(
                        row["source_id"], row["target_id"]
                    )
                    dur = self.distance_service.get_duration(
                        row["source_id"], row["target_id"]
                    )
                except KeyError:
                    dist = None
                    dur = None
                
                distances.append(dist)
                durations.append(dur)
            
            edges["distance_km"] = distances
            edges["duration_h"] = durations
        
        return edges.reset_index(drop=True)
