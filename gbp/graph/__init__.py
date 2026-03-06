"""Universal Graph Model for Optimization Problems.

A flexible, business-agnostic graph data model designed for:
- Logistics optimization
- Supply chain networks
- Transportation systems
- Any graph-based optimization problem

Main classes:
- GraphData: Core container for all graph data
- GraphDataWithQueries: GraphData + query helpers (recommended)
- AttributeTable: Flexible attribute storage with granularity
- FlowsTable: Time-series flow data

Services:
- DistanceService: Compute distances between nodes
- EdgeBuilder: Build edges dynamically

Validation:
- GraphValidator: Validate graph structure
- validate_graph: Convenience function

I/O:
- save_parquet / load_parquet: Parquet format (recommended)
- save_json / load_json: JSON format
- to_dict / from_dict: Dictionary conversion

Example:
    >>> from graph_model import GraphDataWithQueries, AttributeTable
    >>> import pandas as pd
    >>> 
    >>> # Create graph
    >>> graph = GraphDataWithQueries(
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
    >>> 
    >>> # Add attributes
    >>> graph.add_node_attribute(AttributeTable(
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
    ... ))
    >>> 
    >>> # Query
    >>> depots = graph.get_nodes("depot")
    >>> depots_with_costs = graph.nodes_with_attrs("fixed_cost", node_type="depot")
    >>> print(graph.info())
"""

# Core data structures
from gbp.graph.core import (
    GraphData,
    AttributeTable,
    FlowsTable,
    ValueType,
    EntityType,
    AttributeClass,
    TimeGranularity,
    GRANULARITY_TO_FREQ,
)

# Query helpers
from gbp.graph.queries import (
    GraphQueryMixin,
    GraphDataWithQueries,
)

# Builders
from gbp.graph.builders import (
    DistanceService,
    EdgeBuilder,
    DistanceBackend,
)

# Validation
from gbp.graph.validators import (
    GraphValidator,
    ValidationResult,
    ValidationError,
    validate_graph,
)

# I/O
from gbp.graph.io import (
    save_parquet,
    load_parquet,
    save_json,
    load_json,
    to_dict,
    from_dict,
)


__version__ = "0.1.0"

__all__ = [
    # Core
    "GraphData",
    "AttributeTable",
    "FlowsTable",
    "ValueType",
    "EntityType",
    "AttributeClass",
    "TimeGranularity",
    "GRANULARITY_TO_FREQ",
    # Queries
    "GraphQueryMixin",
    "GraphDataWithQueries",
    # Builders
    "DistanceService",
    "EdgeBuilder",
    "DistanceBackend",
    # Validation
    "GraphValidator",
    "ValidationResult",
    "ValidationError",
    "validate_graph",
    # I/O
    "save_parquet",
    "load_parquet",
    "save_json",
    "load_json",
    "to_dict",
    "from_dict",
]
