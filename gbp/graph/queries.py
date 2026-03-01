"""Query helpers for GraphData.

This module extends GraphData with rich query API,
similar to how Pandas provides methods on DataFrames.

Usage:
    from graph_model.core import GraphData
    from graph_model.queries import GraphQueryMixin
    
    # Apply mixin to GraphData
    # Or use the combined class GraphDataWithQueries
"""

from typing import Literal

import pandas as pd

from gbp.graph.core import GraphData, AttributeTable, FlowsTable


class GraphQueryMixin:
    """Mixin class providing query methods for GraphData.
    
    These methods provide convenient access to graph data
    with filtering, merging, and aggregation capabilities.
    """
    
    # Type hints for IDE support (actual attributes come from GraphData)
    nodes: pd.DataFrame
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
    # Merge Helpers (solves Query complexity)
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
        source_ids: list[str] | None = None,
        target_ids: list[str] | None = None,
        source_types: list[str] | None = None,
        target_types: list[str] | None = None,
        *attr_names: str,
        include_self_loops: bool = False,
        **filters
    ) -> pd.DataFrame:
        """Build edges and merge with specified attributes.
        
        Dynamically creates edges between specified nodes
        and joins with edge attributes.
        
        Args:
            source_ids: List of source node IDs. If None, inferred from source_types.
            target_ids: List of target node IDs. If None, inferred from target_types.
            source_types: Node types for sources (used if source_ids is None).
            target_types: Node types for targets (used if target_ids is None).
            *attr_names: Names of edge attributes to merge.
            include_self_loops: Whether to include edges where source=target.
            **filters: Filters for attributes (e.g., commodity_id="bulk").
        
        Returns:
            DataFrame with edges and merged attributes.
        
        Example:
            >>> # Build routes from depots to zones with rates
            >>> routes = graph.edges_with_attrs(
            ...     source_types=["depot"],
            ...     target_types=["zone"],
            ...     "transport_rate", 
            ...     "trip_cost",
            ...     commodity_id="bulk"
            ... )
        """
        # Resolve source_ids
        if source_ids is None:
            if source_types is None:
                source_ids = list(self.nodes["id"])
            else:
                source_ids = list(
                    self.nodes[self.nodes["node_type"].isin(source_types)]["id"]
                )
        
        # Resolve target_ids
        if target_ids is None:
            if target_types is None:
                target_ids = list(self.nodes["id"])
            else:
                target_ids = list(
                    self.nodes[self.nodes["node_type"].isin(target_types)]["id"]
                )
        
        # Build cartesian product
        edges = pd.DataFrame({
            "source_id": [s for s in source_ids for _ in target_ids],
            "target_id": [t for _ in source_ids for t in target_ids],
        })
        
        # Remove self-loops if requested
        if not include_self_loops:
            edges = edges[edges["source_id"] != edges["target_id"]]
        
        # Merge each attribute
        for attr_name in attr_names:
            if attr_name not in self.edge_attributes:
                raise KeyError(f"Edge attribute '{attr_name}' not found")
            
            attr = self.edge_attributes[attr_name]
            attr_df = attr.data.copy()
            
            # Apply filters
            for key, value in filters.items():
                if key in attr_df.columns:
                    attr_df = attr_df[attr_df[key] == value]
            
            # Rename value columns to avoid conflicts
            value_rename = {
                col: f"{attr_name}_{col}" if col in edges.columns else col
                for col in attr.value_columns
            }
            attr_df = attr_df.rename(columns=value_rename)
            
            # Merge on source_id, target_id
            edges = edges.merge(
                attr_df,
                on=["source_id", "target_id"],
                how="left"
            )
        
        return edges
    
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
