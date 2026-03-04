"""Core data structures for Universal Graph Model.

This module contains the fundamental data structures:
- AttributeTable: Flexible attribute storage with granularity metadata
- FlowsTable: Time-series flow data
- GraphData: Main container for all graph data

Design principles:
1. Simple core structure
2. Explicit granularity keys for joins
3. Type information for values
4. Business-agnostic (no domain-specific fields)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from gbp.graph.builders import DistanceService


# =============================================================================
# Type Definitions
# =============================================================================

ValueType = Literal["float", "int", "str", "bool", "json"]
EntityType = Literal["node", "edge"]
AttributeClass = Literal["cost", "rate", "capacity", "tag", "property"]


# =============================================================================
# Attribute Table
# =============================================================================


@dataclass
class AttributeTable:
    """Attribute table with granularity metadata.
    
    Stores attributes for nodes or edges with explicit information about
    what dimensions (granularity keys) the attribute depends on.
    
    Attributes:
        name: Unique identifier for this attribute table.
        entity_type: Whether this attribute applies to "node" or "edge".
        attribute_class: Classification ("cost", "rate", "capacity", "tag", "property").
        granularity_keys: List of columns that define the granularity.
            For nodes: ["node_id"] or ["node_id", "commodity_id", "period"], etc.
            For edges: ["source_id", "target_id"] or with more dimensions.
        value_columns: List of columns containing actual values.
        value_types: Mapping of value column names to their types.
        data: The actual DataFrame with data.
        description: Human-readable description.
    
    Example:
        >>> attr = AttributeTable(
        ...     name="variable_cost",
        ...     entity_type="node",
        ...     attribute_class="cost",
        ...     granularity_keys=["node_id", "commodity_id", "period"],
        ...     value_columns=["value"],
        ...     value_types={"value": "float"},
        ...     data=pd.DataFrame({
        ...         "node_id": ["depot_1", "depot_1"],
        ...         "commodity_id": ["bulk", "cylinder"],
        ...         "period": ["2024-01", "2024-01"],
        ...         "value": [10.0, 15.0],
        ...     }),
        ...     description="Variable processing cost per ton"
        ... )
    """
    
    name: str
    entity_type: EntityType
    attribute_class: AttributeClass
    granularity_keys: list[str]
    value_columns: list[str]
    value_types: dict[str, ValueType]
    data: pd.DataFrame
    description: str = ""
    
    def __post_init__(self) -> None:
        """Validate the attribute table structure."""
        self._validate_columns()
        self._validate_entity_keys()
    
    def _validate_columns(self) -> None:
        """Check that all declared columns exist in data."""
        all_required = set(self.granularity_keys) | set(self.value_columns)
        missing = all_required - set(self.data.columns)
        if missing:
            raise ValueError(
                f"AttributeTable '{self.name}': columns {missing} not found in data. "
                f"Available columns: {list(self.data.columns)}"
            )
    
    def _validate_entity_keys(self) -> None:
        """Check that entity identifier columns are present."""
        if self.entity_type == "node":
            if "node_id" not in self.granularity_keys:
                raise ValueError(
                    f"AttributeTable '{self.name}': node attributes must have "
                    f"'node_id' in granularity_keys"
                )
        elif self.entity_type == "edge":
            if "source_id" not in self.granularity_keys or "target_id" not in self.granularity_keys:
                raise ValueError(
                    f"AttributeTable '{self.name}': edge attributes must have "
                    f"'source_id' and 'target_id' in granularity_keys"
                )
    
    @property
    def all_columns(self) -> list[str]:
        """All columns (keys + values)."""
        return self.granularity_keys + self.value_columns
    
    @property
    def dimension_keys(self) -> list[str]:
        """Granularity keys excluding entity identifiers."""
        if self.entity_type == "node":
            return [k for k in self.granularity_keys if k != "node_id"]
        else:
            return [k for k in self.granularity_keys if k not in ("source_id", "target_id")]
    
    def __len__(self) -> int:
        return len(self.data)
    
    def __repr__(self) -> str:
        return (
            f"AttributeTable(name='{self.name}', "
            f"entity={self.entity_type}, "
            f"class={self.attribute_class}, "
            f"granularity={self.granularity_keys}, "
            f"rows={len(self.data)})"
        )


# =============================================================================
# Flows Table
# =============================================================================


@dataclass
class FlowsTable:
    """Time-series flow data.
    
    Represents flows (movements) between nodes over time.
    
    Attributes:
        name: Unique identifier (e.g., "primary_deliveries", "internal_movements").
        granularity_keys: Columns defining one flow record.
            Typically ["source_id", "target_id", "period"] + optional dimensions.
        value_column: Column name containing the flow value.
        value_type: Type of the value column.
        data: The actual DataFrame.
        description: Human-readable description.
    
    Example:
        >>> flows = FlowsTable(
        ...     name="primary_deliveries",
        ...     granularity_keys=["source_id", "target_id", "commodity_id", "period"],
        ...     value_column="value",
        ...     value_type="float",
        ...     data=pd.DataFrame({
        ...         "source_id": ["depot_1", "depot_1"],
        ...         "target_id": ["zone_001", "zone_002"],
        ...         "commodity_id": ["bulk", "bulk"],
        ...         "period": ["2024-01", "2024-01"],
        ...         "value": [500.0, 300.0],
        ...     }),
        ...     description="Deliveries from depots to customer zones"
        ... )
    """
    
    name: str
    granularity_keys: list[str]
    value_column: str
    value_type: ValueType = "float"
    data: pd.DataFrame = field(default_factory=pd.DataFrame)
    description: str = ""
    
    def __post_init__(self) -> None:
        """Validate the flows table structure."""
        self._validate_columns()
        self._validate_edge_keys()
    
    def _validate_columns(self) -> None:
        """Check that all declared columns exist in data."""
        if len(self.data) == 0:
            return
        
        all_required = set(self.granularity_keys) | {self.value_column}
        missing = all_required - set(self.data.columns)
        if missing:
            raise ValueError(
                f"FlowsTable '{self.name}': columns {missing} not found in data"
            )
    
    def _validate_edge_keys(self) -> None:
        """Check that source_id and target_id are present."""
        if "source_id" not in self.granularity_keys:
            raise ValueError(
                f"FlowsTable '{self.name}': must have 'source_id' in granularity_keys"
            )
        if "target_id" not in self.granularity_keys:
            raise ValueError(
                f"FlowsTable '{self.name}': must have 'target_id' in granularity_keys"
            )
    
    @property
    def dimension_keys(self) -> list[str]:
        """Granularity keys excluding source_id and target_id."""
        return [k for k in self.granularity_keys if k not in ("source_id", "target_id")]
    
    def __len__(self) -> int:
        return len(self.data)
    
    def __repr__(self) -> str:
        return (
            f"FlowsTable(name='{self.name}', "
            f"granularity={self.granularity_keys}, "
            f"rows={len(self.data)})"
        )


# =============================================================================
# Graph Data Container
# =============================================================================


@dataclass
class GraphData:
    """Universal graph container.
    
    Main data structure that holds all graph data:
    - Core entities (nodes, resources, commodities)
    - Geography (coordinates)
    - Attributes (node and edge attributes with flexible granularity)
    - Time-series (flows, demands, inventory, telemetry)
    - Tags (categorical properties)
    
    Design: Simple core structure + rich helper API (see queries module).
    
    Attributes:
        nodes: DataFrame with columns [id, node_type].
        edges: Optional DataFrame with columns [source_id, target_id, ...].
            Populated by build_edges() or set manually.
        resources: Optional DataFrame with columns [id, resource_type].
        commodities: Optional DataFrame with columns [id, commodity_type].
        coordinates: Optional DataFrame with columns [node_id, latitude, longitude].
        node_attributes: Registry of node attribute tables.
        edge_attributes: Registry of edge attribute tables.
        flows: Registry of flow tables.
        demands: Optional DataFrame for demand data.
        inventory: Optional DataFrame for inventory state.
        telemetry: Optional DataFrame for time-series metrics.
        tags: Optional DataFrame for categorical properties.
    
    Example:
        >>> graph = GraphData(
        ...     nodes=pd.DataFrame({
        ...         "id": ["depot_1", "depot_2", "zone_001"],
        ...         "node_type": ["depot", "depot", "zone"],
        ...     }),
        ...     coordinates=pd.DataFrame({
        ...         "node_id": ["depot_1", "depot_2", "zone_001"],
        ...         "latitude": [45.5, 44.8, 45.2],
        ...         "longitude": [9.2, 8.9, 9.5],
        ...     }),
        ... )
    """
    
    # === Core entities ===
    nodes: pd.DataFrame
    edges: pd.DataFrame | None = None
    resources: pd.DataFrame | None = None
    commodities: pd.DataFrame | None = None
    
    # === Geography ===
    coordinates: pd.DataFrame | None = None
    
    # === Attributes (2 registries) ===
    node_attributes: dict[str, AttributeTable] = field(default_factory=dict)
    edge_attributes: dict[str, AttributeTable] = field(default_factory=dict)
    
    # === Time-series ===
    flows: dict[str, FlowsTable] = field(default_factory=dict)
    demands: pd.DataFrame | None = None
    inventory: pd.DataFrame | None = None
    telemetry: pd.DataFrame | None = None
    
    # === Tags (unified) ===
    tags: pd.DataFrame | None = None
    
    # === Distance service (lazy, avoids circular import) ===
    distance_service: DistanceService | None = field(default=None, repr=False)
    
    def __post_init__(self) -> None:
        """Validate graph structure."""
        self._validate_nodes()
        self._validate_coordinates()
        self._validate_attributes()
    
    def _validate_nodes(self) -> None:
        """Validate nodes DataFrame."""
        required_cols = {"id", "node_type"}
        if not required_cols.issubset(self.nodes.columns):
            missing = required_cols - set(self.nodes.columns)
            raise ValueError(f"nodes DataFrame missing columns: {missing}")
        
        if self.nodes["id"].duplicated().any():
            raise ValueError("nodes DataFrame has duplicate ids")
    
    def _validate_coordinates(self) -> None:
        """Validate coordinates DataFrame if present."""
        if self.coordinates is None:
            return
        
        required_cols = {"node_id", "latitude", "longitude"}
        if not required_cols.issubset(self.coordinates.columns):
            missing = required_cols - set(self.coordinates.columns)
            raise ValueError(f"coordinates DataFrame missing columns: {missing}")
        
        # Check referential integrity
        coord_nodes = set(self.coordinates["node_id"])
        valid_nodes = set(self.nodes["id"])
        invalid = coord_nodes - valid_nodes
        if invalid:
            raise ValueError(f"coordinates references unknown nodes: {invalid}")
    
    def _validate_attributes(self) -> None:
        """Validate attribute registries."""
        for name, attr in self.node_attributes.items():
            if attr.entity_type != "node":
                raise ValueError(
                    f"Attribute '{name}' in node_attributes has entity_type='{attr.entity_type}'"
                )
        
        for name, attr in self.edge_attributes.items():
            if attr.entity_type != "edge":
                raise ValueError(
                    f"Attribute '{name}' in edge_attributes has entity_type='{attr.entity_type}'"
                )
    
    # =========================================================================
    # Basic Properties
    # =========================================================================
    
    @property
    def node_ids(self) -> set[str]:
        """Set of all node IDs."""
        return set(self.nodes["id"])
    
    @property
    def node_types(self) -> list[str]:
        """List of unique node types."""
        return self.nodes["node_type"].unique().tolist()
    
    @property
    def resource_ids(self) -> set[str]:
        """Set of all resource IDs."""
        if self.resources is None:
            return set()
        return set(self.resources["id"])
    
    @property
    def commodity_ids(self) -> set[str]:
        """Set of all commodity IDs."""
        if self.commodities is None:
            return set()
        return set(self.commodities["id"])
    
    # =========================================================================
    # Registration Methods
    # =========================================================================
    
    def add_node_attribute(self, attr: AttributeTable) -> None:
        """Add a node attribute table."""
        if attr.entity_type != "node":
            raise ValueError(f"Cannot add edge attribute to node_attributes")
        self.node_attributes[attr.name] = attr
    
    def add_edge_attribute(self, attr: AttributeTable) -> None:
        """Add an edge attribute table."""
        if attr.entity_type != "edge":
            raise ValueError(f"Cannot add node attribute to edge_attributes")
        self.edge_attributes[attr.name] = attr
    
    def add_flows(self, flows_table: FlowsTable) -> None:
        """Add a flows table."""
        self.flows[flows_table.name] = flows_table
    
    # =========================================================================
    # Info / Repr
    # =========================================================================
    
    def __repr__(self) -> str:
        parts = [f"GraphData(nodes={len(self.nodes)}"]
        
        if self.edges is not None:
            parts.append(f"edges={len(self.edges)}")
        if self.resources is not None:
            parts.append(f"resources={len(self.resources)}")
        if self.commodities is not None:
            parts.append(f"commodities={len(self.commodities)}")
        if self.node_attributes:
            parts.append(f"node_attrs={len(self.node_attributes)}")
        if self.edge_attributes:
            parts.append(f"edge_attrs={len(self.edge_attributes)}")
        if self.flows:
            parts.append(f"flows={len(self.flows)}")
        
        return ", ".join(parts) + ")"
