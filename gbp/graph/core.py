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
TimeGranularity = Literal["hourly", "daily", "weekly", "monthly", "quarterly", "yearly"]

GRANULARITY_TO_FREQ: dict[str, str] = {
    "hourly": "h",
    "daily": "D",
    "weekly": "W",
    "monthly": "M",
    "quarterly": "Q",
    "yearly": "Y",
}


# =============================================================================
# Attribute Table
# =============================================================================


@dataclass
class AttributeTable:
    """Attribute table with granularity metadata.
    
    Stores attributes for nodes or edges with explicit information about
    what dimensions (granularity keys) the attribute depends on.
    Supports both static and temporal (time-varying) attributes via
    optional ``date_column`` / ``time_granularity`` fields.
    
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
        date_column: Name of the column containing date/time values.
            Must be present in ``granularity_keys``. ``None`` for static attributes.
        time_granularity: Semantic time resolution of the date column
            ("hourly", "daily", "weekly", "monthly", "quarterly", "yearly").
            Required when ``date_column`` is set; must be ``None`` otherwise.
        date_format: Optional strftime format string for parsing the date column
            (e.g. "%Y-%m", "%Y-%m-%d %H:00"). When ``None``, pandas auto-detection
            is used.
    
    Example (static):
        >>> attr = AttributeTable(
        ...     name="fixed_cost",
        ...     entity_type="node",
        ...     attribute_class="cost",
        ...     granularity_keys=["node_id"],
        ...     value_columns=["value"],
        ...     value_types={"value": "float"},
        ...     data=pd.DataFrame({
        ...         "node_id": ["depot_1", "depot_2"],
        ...         "value": [5000.0, 6000.0],
        ...     }),
        ... )
    
    Example (temporal):
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
        ...     date_column="period",
        ...     time_granularity="monthly",
        ...     date_format="%Y-%m",
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
    date_column: str | None = None
    time_granularity: TimeGranularity | None = None
    date_format: str | None = None
    
    def __post_init__(self) -> None:
        """Validate the attribute table structure."""
        self._validate_columns()
        self._validate_entity_keys()
        self._validate_temporal()
    
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
    
    def _validate_temporal(self) -> None:
        """Check consistency of temporal fields."""
        has_col = self.date_column is not None
        has_gran = self.time_granularity is not None
        
        if has_col != has_gran:
            raise ValueError(
                f"AttributeTable '{self.name}': date_column and time_granularity "
                f"must both be set or both be None"
            )
        
        if not has_col:
            if self.date_format is not None:
                raise ValueError(
                    f"AttributeTable '{self.name}': date_format is set but "
                    f"date_column is None"
                )
            return
        
        if self.date_column not in self.granularity_keys:
            raise ValueError(
                f"AttributeTable '{self.name}': date_column '{self.date_column}' "
                f"must be listed in granularity_keys"
            )
    
    @property
    def is_temporal(self) -> bool:
        """Whether this attribute varies over time."""
        return self.date_column is not None
    
    @property
    def pandas_freq(self) -> str | None:
        """Pandas frequency string corresponding to ``time_granularity``."""
        if self.time_granularity is None:
            return None
        return GRANULARITY_TO_FREQ[self.time_granularity]
    
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
        temporal_info = ""
        if self.is_temporal:
            temporal_info = f", temporal={self.date_column}@{self.time_granularity}"
        return (
            f"AttributeTable(name='{self.name}', "
            f"entity={self.entity_type}, "
            f"class={self.attribute_class}, "
            f"granularity={self.granularity_keys}, "
            f"rows={len(self.data)}"
            f"{temporal_info})"
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
    inventory_ts: pd.DataFrame | None = None
    telemetry_ts: pd.DataFrame | None = None
    timestamps: pd.DatetimeIndex | None = None
    
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

    @property
    def available_dates(self) -> pd.DatetimeIndex:
        """Available timestamps for temporal snapshots."""
        if self.timestamps is not None:
            return self.timestamps
        if self.inventory_ts is not None:
            return pd.DatetimeIndex(self.inventory_ts.index)
        if self.telemetry_ts is not None and "timestamp" in self.telemetry_ts.columns:
            return pd.DatetimeIndex(sorted(pd.to_datetime(self.telemetry_ts["timestamp"]).dropna().unique()))
        return pd.DatetimeIndex([])
    
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
    # Temporal snapshots
    # =========================================================================

    def get_snapshot(self, date: pd.Timestamp) -> GraphData:
        """Return a single-timestamp snapshot from full temporal series."""
        return GraphData(
            nodes=self.nodes.copy(),
            edges=self.edges.copy() if self.edges is not None else None,
            resources=self.resources.copy() if self.resources is not None else None,
            commodities=self.commodities.copy() if self.commodities is not None else None,
            coordinates=self.coordinates.copy() if self.coordinates is not None else None,
            node_attributes=dict(self.node_attributes),
            edge_attributes=dict(self.edge_attributes),
            flows=dict(self.flows),
            demands=self.demands.copy() if self.demands is not None else None,
            inventory=self._snapshot_inventory(date),
            telemetry=self._snapshot_telemetry(date),
            tags=self.tags.copy() if self.tags is not None else None,
            distance_service=self.distance_service,
        )

    def _snapshot_inventory(self, date: pd.Timestamp) -> pd.DataFrame | None:
        if self.inventory_ts is None:
            return self.inventory.copy() if self.inventory is not None else None
        if len(self.inventory_ts.index) == 0:
            return None

        idx = self.inventory_ts.index.get_indexer([date], method="nearest")[0]
        ts = self.inventory_ts.index[idx]
        quantities = self.inventory_ts.loc[ts]
        return pd.DataFrame({
            "node_id": quantities.index.tolist(),
            "commodity_id": "bike",
            "quantity": quantities.values.astype(int),
        })

    def _snapshot_telemetry(self, date: pd.Timestamp) -> pd.DataFrame | None:
        if self.telemetry_ts is None:
            return self.telemetry.copy() if self.telemetry is not None else None
        if self.telemetry_ts.empty or "timestamp" not in self.telemetry_ts.columns:
            return None

        telemetry = self.telemetry_ts.copy()
        telemetry["timestamp"] = pd.to_datetime(telemetry["timestamp"])
        available = pd.DatetimeIndex(sorted(telemetry["timestamp"].dropna().unique()))
        if len(available) == 0:
            return None

        idx = available.get_indexer([date], method="nearest")[0]
        ts = available[idx]
        snap = telemetry[telemetry["timestamp"] == ts].copy()
        if snap.empty or "station_id" not in snap.columns:
            return None

        metric_columns = [
            "num_bikes_available",
            "num_ebikes_available",
            "num_docks_available",
            "num_docks_disabled",
            "num_bikes_disabled",
            "is_installed",
            "is_renting",
            "is_returning",
        ]
        present_metrics = [c for c in metric_columns if c in snap.columns]
        if not present_metrics:
            return None

        normalized = snap.melt(
            id_vars=["station_id", "timestamp"],
            value_vars=present_metrics,
            var_name="metric",
            value_name="value",
        ).rename(columns={"station_id": "node_id"})
        return normalized.reset_index(drop=True)
    
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
            parts.append(f"node_attributes={len(self.node_attributes)}")
        if self.edge_attributes:
            parts.append(f"edge_attributes={len(self.edge_attributes)}")
        if self.flows:
            parts.append(f"flows={len(self.flows)}")
        if self.inventory_ts is not None:
            parts.append(f"inventory_ts={self.inventory_ts.shape[0]}x{self.inventory_ts.shape[1]}")
        if self.telemetry_ts is not None:
            parts.append(f"telemetry_ts={len(self.telemetry_ts)}")
        
        return ", ".join(parts) + ")"
