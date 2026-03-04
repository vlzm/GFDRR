"""I/O functions for GraphData.

This module provides functions to save and load GraphData:
- Parquet format (recommended for performance)
- JSON format (for interoperability)
- In-memory dict (for testing)
"""

import json
from pathlib import Path
from typing import Any

import pandas as pd

from gbp.graph.core import GraphData, AttributeTable, FlowsTable


# =============================================================================
# Parquet I/O (recommended)
# =============================================================================


def save_parquet(graph: GraphData, directory: str | Path) -> None:
    """Save GraphData to Parquet files.
    
    Creates a directory with multiple .parquet files:
    - nodes.parquet
    - edges.parquet (if present)
    - resources.parquet (if present)
    - commodities.parquet (if present)
    - coordinates.parquet (if present)
    - node_attr_{name}.parquet for each node attribute
    - edge_attr_{name}.parquet for each edge attribute
    - flows_{name}.parquet for each flows table
    - demands.parquet (if present)
    - inventory.parquet (if present)
    - telemetry.parquet (if present)
    - tags.parquet (if present)
    - metadata.json (attribute metadata)
    
    Args:
        graph: GraphData to save.
        directory: Directory path to save to.
    
    Example:
        >>> save_parquet(graph, "data/my_graph")
    """
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)
    
    # Core entities
    graph.nodes.to_parquet(path / "nodes.parquet", index=False)
    
    if graph.resources is not None:
        graph.resources.to_parquet(path / "resources.parquet", index=False)
    
    if graph.commodities is not None:
        graph.commodities.to_parquet(path / "commodities.parquet", index=False)
    
    if graph.edges is not None:
        graph.edges.to_parquet(path / "edges.parquet", index=False)
    
    if graph.coordinates is not None:
        graph.coordinates.to_parquet(path / "coordinates.parquet", index=False)
    
    # Attributes
    metadata = {
        "node_attributes": {},
        "edge_attributes": {},
        "flows": {},
    }
    
    for name, attr in graph.node_attributes.items():
        attr.data.to_parquet(path / f"node_attr_{name}.parquet", index=False)
        metadata["node_attributes"][name] = {
            "entity_type": attr.entity_type,
            "attribute_class": attr.attribute_class,
            "granularity_keys": attr.granularity_keys,
            "value_columns": attr.value_columns,
            "value_types": attr.value_types,
            "description": attr.description,
        }
    
    for name, attr in graph.edge_attributes.items():
        attr.data.to_parquet(path / f"edge_attr_{name}.parquet", index=False)
        metadata["edge_attributes"][name] = {
            "entity_type": attr.entity_type,
            "attribute_class": attr.attribute_class,
            "granularity_keys": attr.granularity_keys,
            "value_columns": attr.value_columns,
            "value_types": attr.value_types,
            "description": attr.description,
        }
    
    # Flows
    for name, flow in graph.flows.items():
        flow.data.to_parquet(path / f"flows_{name}.parquet", index=False)
        metadata["flows"][name] = {
            "granularity_keys": flow.granularity_keys,
            "value_column": flow.value_column,
            "value_type": flow.value_type,
            "description": flow.description,
        }
    
    # Time-series
    if graph.demands is not None:
        graph.demands.to_parquet(path / "demands.parquet", index=False)
    
    if graph.inventory is not None:
        graph.inventory.to_parquet(path / "inventory.parquet", index=False)
    
    if graph.telemetry is not None:
        graph.telemetry.to_parquet(path / "telemetry.parquet", index=False)
    
    # Tags
    if graph.tags is not None:
        graph.tags.to_parquet(path / "tags.parquet", index=False)
    
    # Metadata
    with open(path / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)


def load_parquet(directory: str | Path) -> GraphData:
    """Load GraphData from Parquet files.
    
    Args:
        directory: Directory path containing saved files.
    
    Returns:
        Loaded GraphData.
    
    Example:
        >>> graph = load_parquet("data/my_graph")
    """
    path = Path(directory)
    
    # Load metadata
    with open(path / "metadata.json") as f:
        metadata = json.load(f)
    
    # Core entities
    nodes = pd.read_parquet(path / "nodes.parquet")
    
    resources = None
    if (path / "resources.parquet").exists():
        resources = pd.read_parquet(path / "resources.parquet")
    
    commodities = None
    if (path / "commodities.parquet").exists():
        commodities = pd.read_parquet(path / "commodities.parquet")
    
    edges = None
    if (path / "edges.parquet").exists():
        edges = pd.read_parquet(path / "edges.parquet")
    
    coordinates = None
    if (path / "coordinates.parquet").exists():
        coordinates = pd.read_parquet(path / "coordinates.parquet")
    
    # Node attributes
    node_attributes = {}
    for name, meta in metadata["node_attributes"].items():
        data = pd.read_parquet(path / f"node_attr_{name}.parquet")
        node_attributes[name] = AttributeTable(
            name=name,
            entity_type=meta["entity_type"],
            attribute_class=meta["attribute_class"],
            granularity_keys=meta["granularity_keys"],
            value_columns=meta["value_columns"],
            value_types=meta["value_types"],
            data=data,
            description=meta.get("description", ""),
        )
    
    # Edge attributes
    edge_attributes = {}
    for name, meta in metadata["edge_attributes"].items():
        data = pd.read_parquet(path / f"edge_attr_{name}.parquet")
        edge_attributes[name] = AttributeTable(
            name=name,
            entity_type=meta["entity_type"],
            attribute_class=meta["attribute_class"],
            granularity_keys=meta["granularity_keys"],
            value_columns=meta["value_columns"],
            value_types=meta["value_types"],
            data=data,
            description=meta.get("description", ""),
        )
    
    # Flows
    flows = {}
    for name, meta in metadata["flows"].items():
        data = pd.read_parquet(path / f"flows_{name}.parquet")
        flows[name] = FlowsTable(
            name=name,
            granularity_keys=meta["granularity_keys"],
            value_column=meta["value_column"],
            value_type=meta.get("value_type", "float"),
            data=data,
            description=meta.get("description", ""),
        )
    
    # Time-series
    demands = None
    if (path / "demands.parquet").exists():
        demands = pd.read_parquet(path / "demands.parquet")
    
    inventory = None
    if (path / "inventory.parquet").exists():
        inventory = pd.read_parquet(path / "inventory.parquet")
    
    telemetry = None
    if (path / "telemetry.parquet").exists():
        telemetry = pd.read_parquet(path / "telemetry.parquet")
    
    # Tags
    tags = None
    if (path / "tags.parquet").exists():
        tags = pd.read_parquet(path / "tags.parquet")
    
    return GraphData(
        nodes=nodes,
        edges=edges,
        resources=resources,
        commodities=commodities,
        coordinates=coordinates,
        node_attributes=node_attributes,
        edge_attributes=edge_attributes,
        flows=flows,
        demands=demands,
        inventory=inventory,
        telemetry=telemetry,
        tags=tags,
    )


# =============================================================================
# Dict I/O (for testing and serialization)
# =============================================================================


def to_dict(graph: GraphData) -> dict[str, Any]:
    """Convert GraphData to dictionary.
    
    DataFrames are converted to list of dicts.
    
    Args:
        graph: GraphData to convert.
    
    Returns:
        Dictionary representation.
    
    Example:
        >>> d = to_dict(graph)
        >>> json.dumps(d)  # can be serialized to JSON
    """
    result: dict[str, Any] = {
        "nodes": graph.nodes.to_dict(orient="records"),
    }
    
    if graph.edges is not None:
        result["edges"] = graph.edges.to_dict(orient="records")
    
    if graph.resources is not None:
        result["resources"] = graph.resources.to_dict(orient="records")
    
    if graph.commodities is not None:
        result["commodities"] = graph.commodities.to_dict(orient="records")
    
    if graph.coordinates is not None:
        result["coordinates"] = graph.coordinates.to_dict(orient="records")
    
    # Attributes
    result["node_attributes"] = {}
    for name, attr in graph.node_attributes.items():
        result["node_attributes"][name] = {
            "entity_type": attr.entity_type,
            "attribute_class": attr.attribute_class,
            "granularity_keys": attr.granularity_keys,
            "value_columns": attr.value_columns,
            "value_types": attr.value_types,
            "description": attr.description,
            "data": attr.data.to_dict(orient="records"),
        }
    
    result["edge_attributes"] = {}
    for name, attr in graph.edge_attributes.items():
        result["edge_attributes"][name] = {
            "entity_type": attr.entity_type,
            "attribute_class": attr.attribute_class,
            "granularity_keys": attr.granularity_keys,
            "value_columns": attr.value_columns,
            "value_types": attr.value_types,
            "description": attr.description,
            "data": attr.data.to_dict(orient="records"),
        }
    
    # Flows
    result["flows"] = {}
    for name, flow in graph.flows.items():
        result["flows"][name] = {
            "granularity_keys": flow.granularity_keys,
            "value_column": flow.value_column,
            "value_type": flow.value_type,
            "description": flow.description,
            "data": flow.data.to_dict(orient="records"),
        }
    
    # Time-series
    if graph.demands is not None:
        result["demands"] = graph.demands.to_dict(orient="records")
    
    if graph.inventory is not None:
        result["inventory"] = graph.inventory.to_dict(orient="records")
    
    if graph.telemetry is not None:
        result["telemetry"] = graph.telemetry.to_dict(orient="records")
    
    if graph.tags is not None:
        result["tags"] = graph.tags.to_dict(orient="records")
    
    return result


def from_dict(data: dict[str, Any]) -> GraphData:
    """Create GraphData from dictionary.
    
    Args:
        data: Dictionary representation (from to_dict).
    
    Returns:
        GraphData instance.
    
    Example:
        >>> graph = from_dict(json.loads(json_string))
    """
    nodes = pd.DataFrame(data["nodes"])
    
    edges = None
    if "edges" in data:
        edges = pd.DataFrame(data["edges"])
    
    resources = None
    if "resources" in data:
        resources = pd.DataFrame(data["resources"])
    
    commodities = None
    if "commodities" in data:
        commodities = pd.DataFrame(data["commodities"])
    
    coordinates = None
    if "coordinates" in data:
        coordinates = pd.DataFrame(data["coordinates"])
    
    # Node attributes
    node_attributes = {}
    for name, attr_data in data.get("node_attributes", {}).items():
        node_attributes[name] = AttributeTable(
            name=name,
            entity_type=attr_data["entity_type"],
            attribute_class=attr_data["attribute_class"],
            granularity_keys=attr_data["granularity_keys"],
            value_columns=attr_data["value_columns"],
            value_types=attr_data["value_types"],
            data=pd.DataFrame(attr_data["data"]),
            description=attr_data.get("description", ""),
        )
    
    # Edge attributes
    edge_attributes = {}
    for name, attr_data in data.get("edge_attributes", {}).items():
        edge_attributes[name] = AttributeTable(
            name=name,
            entity_type=attr_data["entity_type"],
            attribute_class=attr_data["attribute_class"],
            granularity_keys=attr_data["granularity_keys"],
            value_columns=attr_data["value_columns"],
            value_types=attr_data["value_types"],
            data=pd.DataFrame(attr_data["data"]),
            description=attr_data.get("description", ""),
        )
    
    # Flows
    flows = {}
    for name, flow_data in data.get("flows", {}).items():
        flows[name] = FlowsTable(
            name=name,
            granularity_keys=flow_data["granularity_keys"],
            value_column=flow_data["value_column"],
            value_type=flow_data.get("value_type", "float"),
            data=pd.DataFrame(flow_data["data"]),
            description=flow_data.get("description", ""),
        )
    
    # Time-series
    demands = None
    if "demands" in data:
        demands = pd.DataFrame(data["demands"])
    
    inventory = None
    if "inventory" in data:
        inventory = pd.DataFrame(data["inventory"])
    
    telemetry = None
    if "telemetry" in data:
        telemetry = pd.DataFrame(data["telemetry"])
    
    tags = None
    if "tags" in data:
        tags = pd.DataFrame(data["tags"])
    
    return GraphData(
        nodes=nodes,
        edges=edges,
        resources=resources,
        commodities=commodities,
        coordinates=coordinates,
        node_attributes=node_attributes,
        edge_attributes=edge_attributes,
        flows=flows,
        demands=demands,
        inventory=inventory,
        telemetry=telemetry,
        tags=tags,
    )


# =============================================================================
# JSON I/O (convenience wrappers)
# =============================================================================


def save_json(graph: GraphData, filepath: str | Path) -> None:
    """Save GraphData to JSON file.
    
    Args:
        graph: GraphData to save.
        filepath: Path to JSON file.
    
    Example:
        >>> save_json(graph, "data/my_graph.json")
    """
    data = to_dict(graph)
    
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=str)


def load_json(filepath: str | Path) -> GraphData:
    """Load GraphData from JSON file.
    
    Args:
        filepath: Path to JSON file.
    
    Returns:
        Loaded GraphData.
    
    Example:
        >>> graph = load_json("data/my_graph.json")
    """
    with open(filepath) as f:
        data = json.load(f)
    
    return from_dict(data)
