"""Query helpers for GraphData.

This module extends GraphData with rich query API,
similar to how Pandas provides methods on DataFrames.

Usage:
    from graph_model.core import GraphData
    from graph_model.queries import GraphQueryMixin
    
    # Apply mixin to GraphData
    # Or use the combined class GraphDataWithQueries
"""

from __future__ import annotations

from typing import Literal

import pandas as pd

from gbp.graph.core import GraphData, AttributeTable, FlowsTable
from gbp.graph.builders import DistanceService, DistanceBackend, EdgeBuilder


class GraphQueryMixin:
    """Mixin class providing query methods for GraphData.
    
    These methods provide convenient access to graph data
    with filtering, merging, and aggregation capabilities.
    """
    
    # Type hints for IDE support (actual attributes come from GraphData)
    nodes: pd.DataFrame
    edges: pd.DataFrame | None
    resources: pd.DataFrame | None
    commodities: pd.DataFrame | None
    coordinates: pd.DataFrame | None
    node_attributes: dict[str, AttributeTable]
    edge_attributes: dict[str, AttributeTable]
    flows: dict[str, FlowsTable]
    demands: pd.DataFrame | None
    inventory: pd.DataFrame | None
    telemetry: pd.DataFrame | None
    tags: pd.DataFrame | None
    distance_service: DistanceService | None
    
    # =========================================================================
    # Node Queries
    # =========================================================================
    
    def get_nodes(self, node_type: str | None = None) -> pd.DataFrame:
        """Get nodes, optionally filtered by type.
        
        Args:
            node_type: If provided, filter to nodes of this type.
        
        Returns:
            DataFrame with node data.
        
        Example:
            >>> depots = graph.get_nodes("depot")
            >>> all_nodes = graph.get_nodes()
        """
        if node_type is None:
            return self.nodes.copy()
        return self.nodes[self.nodes["node_type"] == node_type].copy()
    
    def get_nodes_by_ids(self, node_ids: list[str]) -> pd.DataFrame:
        """Get nodes by their IDs.
        
        Args:
            node_ids: List of node IDs to retrieve.
        
        Returns:
            DataFrame with matching nodes.
        """
        return self.nodes[self.nodes["id"].isin(node_ids)].copy()
    
    # =========================================================================
    # Attribute Queries
    # =========================================================================
    
    def get_node_attr(
        self,
        name: str,
        **filters
    ) -> pd.DataFrame:
        """Get node attribute data with optional filters.
        
        Args:
            name: Name of the attribute table.
            **filters: Column=value filters to apply.
        
        Returns:
            Filtered DataFrame.
        
        Example:
            >>> costs = graph.get_node_attr("variable_cost")
            >>> costs = graph.get_node_attr("variable_cost", commodity_id="bulk")
            >>> costs = graph.get_node_attr("variable_cost", 
            ...                             commodity_id="bulk", 
            ...                             period="2024-01")
        """
        if name not in self.node_attributes:
            raise KeyError(
                f"Node attribute '{name}' not found. "
                f"Available: {list(self.node_attributes.keys())}"
            )
        
        df = self.node_attributes[name].data.copy()
        
        for key, value in filters.items():
            if key in df.columns:
                df = df[df[key] == value]
        
        return df
    
    def get_edge_attr(
        self,
        name: str,
        **filters
    ) -> pd.DataFrame:
        """Get edge attribute data with optional filters.
        
        Args:
            name: Name of the attribute table.
            **filters: Column=value filters to apply.
        
        Returns:
            Filtered DataFrame.
        
        Example:
            >>> rates = graph.get_edge_attr("transport_rate")
            >>> rates = graph.get_edge_attr("transport_rate", commodity_id="bulk")
        """
        if name not in self.edge_attributes:
            raise KeyError(
                f"Edge attribute '{name}' not found. "
                f"Available: {list(self.edge_attributes.keys())}"
            )
        
        df = self.edge_attributes[name].data.copy()
        
        for key, value in filters.items():
            if key in df.columns:
                df = df[df[key] == value]
        
        return df
    
    def list_node_attrs(
        self,
        attribute_class: str | None = None
    ) -> list[str]:
        """List available node attributes.
        
        Args:
            attribute_class: If provided, filter by class 
                ("cost", "rate", "capacity", "tag", "property").
        
        Returns:
            List of attribute names.
        """
        if attribute_class is None:
            return list(self.node_attributes.keys())
        return [
            name for name, attr in self.node_attributes.items()
            if attr.attribute_class == attribute_class
        ]
    
    def list_edge_attrs(
        self,
        attribute_class: str | None = None
    ) -> list[str]:
        """List available edge attributes.
        
        Args:
            attribute_class: If provided, filter by class.
        
        Returns:
            List of attribute names.
        """
        if attribute_class is None:
            return list(self.edge_attributes.keys())
        return [
            name for name, attr in self.edge_attributes.items()
            if attr.attribute_class == attribute_class
        ]
    
    # =========================================================================
    # Distance Service
    # =========================================================================
    
    def set_distance_service(
        self,
        service: DistanceService | None = None,
        *,
        backend: DistanceBackend = "haversine",
        default_speed_kmh: float = 50.0,
    ) -> None:
        """Configure distance calculations for edge building.
        
        Either pass an existing DistanceService, or let the graph create one
        from its own coordinates.
        
        Args:
            service: Pre-configured DistanceService. If None, creates one
                from the graph's coordinates.
            backend: Distance backend (used when creating from coordinates).
            default_speed_kmh: Default speed for duration estimates.
        
        Example:
            >>> graph.set_distance_service(backend="haversine")
            >>> graph.set_distance_service(my_custom_service)
        """
        if service is not None:
            self.distance_service = service
            return
        
        if self.coordinates is None:
            raise ValueError(
                "Cannot create DistanceService: no coordinates in graph. "
                "Either set coordinates or pass an existing service."
            )
        
        self.distance_service = DistanceService(
            coordinates=self.coordinates,
            backend=backend,
            default_speed_kmh=default_speed_kmh,
        )
    
    def _get_edge_builder(
        self,
        distance_service: DistanceService | None = None,
    ) -> EdgeBuilder:
        """Create an EdgeBuilder bound to this graph.
        
        Args:
            distance_service: Override for the graph's distance_service.
        """
        svc = distance_service or self.distance_service
        return EdgeBuilder(graph=self, distance_service=svc)  # type: ignore[arg-type]
    
    # =========================================================================
    # Edge Building
    # =========================================================================
    
    def build_edges(
        self,
        *,
        source_ids: list[str] | None = None,
        target_ids: list[str] | None = None,
        source_types: list[str] | None = None,
        target_types: list[str] | None = None,
        include_distances: bool = True,
        include_self_loops: bool = False,
        bidirectional: bool = False,
        distance_service: DistanceService | None = None,
        store: bool = True,
    ) -> pd.DataFrame:
        """Build edges between nodes and store on the graph.
        
        Creates edges dynamically between specified source and target nodes.
        Delegates to EdgeBuilder internally. By default the result is stored
        in ``self.edges``.
        
        Args:
            source_ids: Explicit list of source node IDs.
            target_ids: Explicit list of target node IDs.
            source_types: Node types for sources (used if source_ids is None).
            target_types: Node types for targets (used if target_ids is None).
            include_distances: Whether to compute distances (requires distance_service).
            include_self_loops: Whether to include edges where source=target.
            bidirectional: If True, include both A->B and B->A edges.
            distance_service: Override distance service for this call.
            store: If True (default), save result to ``self.edges``.
        
        Returns:
            DataFrame with columns: source_id, target_id, [distance_km, duration_h].
        
        Example:
            >>> graph.set_distance_service(backend="haversine")
            >>> graph.build_edges(
            ...     source_types=["depot"],
            ...     target_types=["zone"]
            ... )
            >>> graph.edges  # edges are now stored
        """
        builder = self._get_edge_builder(distance_service)
        edges = builder.build(
            source_ids=source_ids,
            target_ids=target_ids,
            source_types=source_types,
            target_types=target_types,
            include_distances=include_distances,
            include_self_loops=include_self_loops,
            bidirectional=bidirectional,
        )
        if store:
            self.edges = edges
        return edges
    
    def build_edges_from_flows(
        self,
        flow_name: str,
        *,
        include_distances: bool = True,
        distance_service: DistanceService | None = None,
        store: bool = True,
        **filters,
    ) -> pd.DataFrame:
        """Build edges based on existing flow data (sparse graph).
        
        Creates edges only where flows exist, rather than the full
        cartesian product. By default the result is stored in ``self.edges``.
        
        Args:
            flow_name: Name of the flows table.
            include_distances: Whether to compute distances.
            distance_service: Override distance service for this call.
            store: If True (default), save result to ``self.edges``.
            **filters: Filters for flows (e.g., commodity_id="bulk").
        
        Returns:
            DataFrame with edges derived from flows.
        
        Example:
            >>> graph.build_edges_from_flows(
            ...     "primary_deliveries",
            ...     commodity_id="bulk"
            ... )
            >>> graph.edges  # edges are now stored
        """
        builder = self._get_edge_builder(distance_service)
        edges = builder.build_from_flows(
            flow_name,
            include_distances=include_distances,
            **filters,
        )
        if store:
            self.edges = edges
        return edges
    
    # =========================================================================
    # Merge Helpers
    # =========================================================================
    
    def nodes_with_attrs(
        self,
        *attr_names: str,
        node_type: str | None = None,
        **filters
    ) -> pd.DataFrame:
        """Get nodes merged with specified attributes.
        
        This is the main convenience method for getting node data
        with multiple attributes joined together.
        
        Args:
            *attr_names: Names of node attributes to merge.
            node_type: If provided, filter nodes by type.
            **filters: Filters to apply to attributes (e.g., commodity_id="bulk").
        
        Returns:
            DataFrame with nodes and merged attributes.
        
        Example:
            >>> # Get depots with costs and capacity
            >>> df = graph.nodes_with_attrs(
            ...     "fixed_cost", 
            ...     "variable_cost", 
            ...     "monthly_capacity",
            ...     node_type="depot",
            ...     commodity_id="bulk",
            ...     period="2024-01"
            ... )
        """
        # Start with nodes
        df = self.get_nodes(node_type)
        
        # Merge each attribute
        for attr_name in attr_names:
            if attr_name not in self.node_attributes:
                raise KeyError(f"Node attribute '{attr_name}' not found")
            
            attr = self.node_attributes[attr_name]
            attr_df = attr.data.copy()
            
            # Apply filters to attribute data
            for key, value in filters.items():
                if key in attr_df.columns:
                    attr_df = attr_df[attr_df[key] == value]
            
            # Rename value columns to avoid conflicts
            value_rename = {
                col: f"{attr_name}_{col}" if col in df.columns else col
                for col in attr.value_columns
            }
            attr_df = attr_df.rename(columns=value_rename)
            
            # Merge on node_id = id
            df = df.merge(
                attr_df,
                left_on="id",
                right_on="node_id",
                how="left"
            )
            
            # Drop redundant node_id column
            if "node_id" in df.columns:
                df = df.drop(columns=["node_id"])
        
        return df
    
    def edges_with_attrs(
        self,
        *attr_names: str,
        source_ids: list[str] | None = None,
        target_ids: list[str] | None = None,
        source_types: list[str] | None = None,
        target_types: list[str] | None = None,
        include_distances: bool = True,
        include_self_loops: bool = False,
        distance_service: DistanceService | None = None,
        **filters,
    ) -> pd.DataFrame:
        """Build edges and merge with specified attributes.
        
        Dynamically creates edges between specified nodes, computes
        distances, and joins with edge attributes. Delegates to
        EdgeBuilder internally.
        
        Args:
            *attr_names: Names of edge attributes to merge.
            source_ids: Explicit list of source node IDs.
            target_ids: Explicit list of target node IDs.
            source_types: Node types for sources (used if source_ids is None).
            target_types: Node types for targets (used if target_ids is None).
            include_distances: Whether to compute distances.
            include_self_loops: Whether to include edges where source=target.
            distance_service: Override distance service for this call.
            **filters: Filters for attributes (e.g., commodity_id="bulk").
        
        Returns:
            DataFrame with edges and merged attributes.
        
        Example:
            >>> routes = graph.edges_with_attrs(
            ...     "transport_rate", "trip_cost",
            ...     source_types=["depot"],
            ...     target_types=["zone"],
            ...     commodity_id="bulk",
            ... )
        """
        builder = self._get_edge_builder(distance_service)
        return builder.build_with_attrs(
            source_ids=source_ids,
            target_ids=target_ids,
            source_types=source_types,
            target_types=target_types,
            attr_names=list(attr_names) if attr_names else None,
            include_distances=include_distances,
            include_self_loops=include_self_loops,
            **filters,
        )
    
    # =========================================================================
    # Tag Queries
    # =========================================================================
    
    def get_tag(
        self,
        entity_type: Literal["node", "resource", "commodity"],
        entity_id: str,
        key: str
    ) -> str | None:
        """Get single tag value.
        
        Args:
            entity_type: Type of entity.
            entity_id: ID of the entity.
            key: Tag key.
        
        Returns:
            Tag value or None if not found.
        """
        if self.tags is None:
            return None
        
        mask = (
            (self.tags["entity_type"] == entity_type) &
            (self.tags["entity_id"] == entity_id) &
            (self.tags["key"] == key)
        )
        matches = self.tags[mask]
        
        if len(matches) == 0:
            return None
        return matches["value"].iloc[0]
    
    def get_tags_for(
        self,
        entity_type: Literal["node", "resource", "commodity"],
        entity_id: str
    ) -> dict[str, str]:
        """Get all tags for an entity.
        
        Args:
            entity_type: Type of entity.
            entity_id: ID of the entity.
        
        Returns:
            Dictionary of tag key -> value.
        """
        if self.tags is None:
            return {}
        
        mask = (
            (self.tags["entity_type"] == entity_type) &
            (self.tags["entity_id"] == entity_id)
        )
        matches = self.tags[mask]
        
        return dict(zip(matches["key"], matches["value"]))
    
    def nodes_with_tags(
        self,
        *tag_keys: str,
        node_type: str | None = None
    ) -> pd.DataFrame:
        """Get nodes with specified tags pivoted as columns.
        
        Args:
            *tag_keys: Tag keys to include as columns.
            node_type: If provided, filter nodes by type.
        
        Returns:
            DataFrame with nodes and tag columns.
        
        Example:
            >>> df = graph.nodes_with_tags("region", "ownership", node_type="depot")
            >>> # Returns: id, node_type, region, ownership
        """
        df = self.get_nodes(node_type)
        
        if self.tags is None:
            return df
        
        # Filter to node tags
        node_tags = self.tags[self.tags["entity_type"] == "node"]
        
        # Filter to requested keys
        if tag_keys:
            node_tags = node_tags[node_tags["key"].isin(tag_keys)]
        
        # Pivot tags to columns
        if len(node_tags) > 0:
            tags_pivot = node_tags.pivot(
                index="entity_id",
                columns="key",
                values="value"
            ).reset_index()
            tags_pivot = tags_pivot.rename(columns={"entity_id": "id"})
            
            df = df.merge(tags_pivot, on="id", how="left")
        
        return df
    
    # =========================================================================
    # Flow Queries
    # =========================================================================
    
    def get_flows(
        self,
        name: str,
        **filters
    ) -> pd.DataFrame:
        """Get flow data with optional filters.
        
        Args:
            name: Name of the flows table.
            **filters: Column=value filters.
        
        Returns:
            Filtered DataFrame.
        
        Example:
            >>> deliveries = graph.get_flows("primary_deliveries")
            >>> jan_bulk = graph.get_flows("primary_deliveries", 
            ...                            period="2024-01", 
            ...                            commodity_id="bulk")
        """
        if name not in self.flows:
            raise KeyError(
                f"Flows '{name}' not found. "
                f"Available: {list(self.flows.keys())}"
            )
        
        df = self.flows[name].data.copy()
        
        for key, value in filters.items():
            if key in df.columns:
                df = df[df[key] == value]
        
        return df
    
    def aggregate_flows(
        self,
        name: str,
        group_by: list[str],
        agg_func: str = "sum",
        **filters
    ) -> pd.DataFrame:
        """Aggregate flow data.
        
        Args:
            name: Name of the flows table.
            group_by: Columns to group by.
            agg_func: Aggregation function ("sum", "mean", "count", etc.).
            **filters: Pre-aggregation filters.
        
        Returns:
            Aggregated DataFrame.
        
        Example:
            >>> # Total deliveries by depot
            >>> totals = graph.aggregate_flows(
            ...     "primary_deliveries",
            ...     group_by=["source_id"],
            ...     period="2024-01"
            ... )
        """
        df = self.get_flows(name, **filters)
        
        flows_table = self.flows[name]
        value_col = flows_table.value_column
        
        return df.groupby(group_by, as_index=False).agg({value_col: agg_func})
    
    # =========================================================================
    # Coordinate Queries
    # =========================================================================
    
    def get_coordinates(
        self,
        node_ids: list[str] | None = None,
        node_type: str | None = None
    ) -> pd.DataFrame:
        """Get coordinates for nodes.
        
        Args:
            node_ids: If provided, filter to these nodes.
            node_type: If provided, filter to nodes of this type.
        
        Returns:
            DataFrame with node_id, latitude, longitude.
        """
        if self.coordinates is None:
            raise ValueError("No coordinates available in this graph")
        
        df = self.coordinates.copy()
        
        if node_ids is not None:
            df = df[df["node_id"].isin(node_ids)]
        
        if node_type is not None:
            type_nodes = set(self.get_nodes(node_type)["id"])
            df = df[df["node_id"].isin(type_nodes)]
        
        return df
    
    def nodes_with_coordinates(
        self,
        node_type: str | None = None
    ) -> pd.DataFrame:
        """Get nodes merged with their coordinates.
        
        Args:
            node_type: If provided, filter to nodes of this type.
        
        Returns:
            DataFrame with id, node_type, latitude, longitude.
        """
        df = self.get_nodes(node_type)
        
        if self.coordinates is None:
            return df
        
        return df.merge(
            self.coordinates,
            left_on="id",
            right_on="node_id",
            how="left"
        ).drop(columns=["node_id"], errors="ignore")
    
    # =========================================================================
    # Info / Describe
    # =========================================================================
    
    def info(self) -> str:
        """Summary of graph contents (like df.info()).
        
        Returns:
            Multi-line string with graph summary.
        """
        lines = [
            "GraphData",
            f"  Nodes: {len(self.nodes)} ({', '.join(self.node_types)})",
        ]
        
        if self.edges is not None:
            lines.append(f"  Edges: {len(self.edges)}")
        
        if self.resources is not None:
            types = self.resources["resource_type"].unique().tolist()
            lines.append(f"  Resources: {len(self.resources)} ({', '.join(types)})")
        
        if self.commodities is not None:
            types = self.commodities["commodity_type"].unique().tolist()
            lines.append(f"  Commodities: {len(self.commodities)} ({', '.join(types)})")
        
        if self.coordinates is not None:
            lines.append(f"  Coordinates: {len(self.coordinates)} nodes")
        
        if self.node_attributes:
            lines.append("  Node attributes:")
            for name, attr in self.node_attributes.items():
                lines.append(f"    - {name} ({attr.attribute_class}): {len(attr)} rows")
        
        if self.edge_attributes:
            lines.append("  Edge attributes:")
            for name, attr in self.edge_attributes.items():
                lines.append(f"    - {name} ({attr.attribute_class}): {len(attr)} rows")
        
        if self.flows:
            lines.append("  Flows:")
            for name, flow in self.flows.items():
                lines.append(f"    - {name}: {len(flow)} rows")
        
        if self.demands is not None:
            lines.append(f"  Demands: {len(self.demands)} rows")
        if self.inventory is not None:
            lines.append(f"  Inventory: {len(self.inventory)} rows")
        if self.telemetry is not None:
            lines.append(f"  Telemetry: {len(self.telemetry)} rows")
        if self.tags is not None:
            lines.append(f"  Tags: {len(self.tags)} entries")
        
        return "\n".join(lines)
    
    def describe_attr(self, name: str) -> str:
        """Describe an attribute table.
        
        Args:
            name: Name of the attribute (node or edge).
        
        Returns:
            Multi-line description string.
        """
        attr = None
        
        if name in self.node_attributes:
            attr = self.node_attributes[name]
        elif name in self.edge_attributes:
            attr = self.edge_attributes[name]
        else:
            return f"Attribute '{name}' not found"
        
        return (
            f"AttributeTable: {attr.name}\n"
            f"  Entity type: {attr.entity_type}\n"
            f"  Class: {attr.attribute_class}\n"
            f"  Granularity keys: {attr.granularity_keys}\n"
            f"  Value columns: {attr.value_columns}\n"
            f"  Value types: {attr.value_types}\n"
            f"  Rows: {len(attr.data)}\n"
            f"  Description: {attr.description or '(none)'}"
        )
    
    def describe_flows(self, name: str) -> str:
        """Describe a flows table.
        
        Args:
            name: Name of the flows table.
        
        Returns:
            Multi-line description string.
        """
        if name not in self.flows:
            return f"Flows '{name}' not found"
        
        flow = self.flows[name]
        
        return (
            f"FlowsTable: {flow.name}\n"
            f"  Granularity keys: {flow.granularity_keys}\n"
            f"  Value column: {flow.value_column} ({flow.value_type})\n"
            f"  Rows: {len(flow.data)}\n"
            f"  Description: {flow.description or '(none)'}"
        )


# =============================================================================
# Combined Class (convenience)
# =============================================================================


class GraphDataWithQueries(GraphQueryMixin, GraphData):
    """GraphData with all query methods included.
    
    This is the recommended class to use in most cases.
    It combines the core GraphData structure with all query helpers.
    
    Example:
        >>> graph = GraphDataWithQueries(
        ...     nodes=pd.DataFrame({
        ...         "id": ["depot_1", "zone_001"],
        ...         "node_type": ["depot", "zone"],
        ...     })
        ... )
        >>> graph.info()
        >>> graph.get_nodes("depot")
    """
    pass
