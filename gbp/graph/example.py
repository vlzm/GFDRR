"""Example: Building a logistics network graph.

This example demonstrates how to use the graph model
for a supply chain / logistics optimization scenario.
"""

import pandas as pd

from gbp.graph import (
    GraphDataWithQueries,
    AttributeTable,
    FlowsTable,
    validate_graph,
    save_parquet,
)


def create_logistics_graph() -> GraphDataWithQueries:
    """Create example logistics network graph."""
    
    # =========================================================================
    # Core Entities
    # =========================================================================
    
    # Nodes: terminals, depots, customer zones
    nodes = pd.DataFrame({
        "id": [
            "terminal_rome", "terminal_milan",
            "depot_naples", "depot_florence", "depot_bologna",
            "zone_001", "zone_002", "zone_003", "zone_004",
        ],
        "node_type": [
            "terminal", "terminal",
            "depot", "depot", "depot",
            "zone", "zone", "zone", "zone",
        ],
    })
    
    # Resources: vehicles
    resources = pd.DataFrame({
        "id": ["truck_large", "truck_small", "tanker"],
        "resource_type": ["vehicle", "vehicle", "vehicle"],
    })
    
    # Commodities: product types
    commodities = pd.DataFrame({
        "id": ["bulk_lpg", "bulk_lng", "cylinder_propane", "cylinder_mix"],
        "commodity_type": ["bulk", "bulk", "cylinder", "cylinder"],
    })
    
    # Coordinates
    coordinates = pd.DataFrame({
        "node_id": [
            "terminal_rome", "terminal_milan",
            "depot_naples", "depot_florence", "depot_bologna",
            "zone_001", "zone_002", "zone_003", "zone_004",
        ],
        "latitude": [
            41.9, 45.5,
            40.8, 43.8, 44.5,
            41.0, 42.5, 44.0, 45.0,
        ],
        "longitude": [
            12.5, 9.2,
            14.2, 11.2, 11.3,
            14.5, 12.0, 11.0, 10.0,
        ],
    })
    
    # Tags: categorical properties
    tags = pd.DataFrame({
        "entity_type": [
            "node", "node", "node", "node", "node",
            "resource", "resource", "resource",
            "commodity", "commodity",
        ],
        "entity_id": [
            "depot_naples", "depot_florence", "depot_bologna",
            "terminal_rome", "terminal_milan",
            "truck_large", "truck_small", "tanker",
            "bulk_lpg", "cylinder_propane",
        ],
        "key": [
            "region", "region", "region",
            "ownership", "ownership",
            "fuel_type", "fuel_type", "fuel_type",
            "hazard_class", "hazard_class",
        ],
        "value": [
            "south", "central", "north",
            "owned", "3rd_party",
            "diesel", "diesel", "diesel",
            "2.1", "2.1",
        ],
    })
    
    # =========================================================================
    # Create Graph
    # =========================================================================
    
    graph = GraphDataWithQueries(
        nodes=nodes,
        resources=resources,
        commodities=commodities,
        coordinates=coordinates,
        tags=tags,
    )
    
    # =========================================================================
    # Node Attributes
    # =========================================================================
    
    # Fixed costs (simple granularity: just node_id)
    graph.add_node_attribute(AttributeTable(
        name="fixed_cost",
        entity_type="node",
        attribute_class="cost",
        granularity_keys=["node_id"],
        value_columns=["value"],
        value_types={"value": "float"},
        data=pd.DataFrame({
            "node_id": ["depot_naples", "depot_florence", "depot_bologna"],
            "value": [50000.0, 45000.0, 55000.0],
        }),
        description="Annual fixed operating cost (EUR)",
    ))
    
    # Variable costs (granularity: node_id + commodity_id)
    graph.add_node_attribute(AttributeTable(
        name="variable_cost",
        entity_type="node",
        attribute_class="cost",
        granularity_keys=["node_id", "commodity_id"],
        value_columns=["value"],
        value_types={"value": "float"},
        data=pd.DataFrame({
            "node_id": [
                "depot_naples", "depot_naples",
                "depot_florence", "depot_florence",
                "depot_bologna", "depot_bologna",
            ],
            "commodity_id": [
                "bulk_lpg", "cylinder_propane",
                "bulk_lpg", "cylinder_propane",
                "bulk_lpg", "cylinder_propane",
            ],
            "value": [8.0, 12.0, 7.5, 11.0, 9.0, 13.0],
        }),
        description="Variable processing cost (EUR/ton)",
    ))
    
    # COGS rates (granularity: node_id + commodity_id + period)
    graph.add_node_attribute(AttributeTable(
        name="cogs_rate",
        entity_type="node",
        attribute_class="rate",
        granularity_keys=["node_id", "commodity_id", "period"],
        value_columns=["value"],
        value_types={"value": "float"},
        data=pd.DataFrame({
            "node_id": [
                "terminal_rome", "terminal_rome",
                "terminal_milan", "terminal_milan",
            ],
            "commodity_id": ["bulk_lpg", "bulk_lpg", "bulk_lng", "bulk_lng"],
            "period": ["2024-01", "2024-02", "2024-01", "2024-02"],
            "value": [380.0, 385.0, 420.0, 425.0],
        }),
        description="Cost of goods sold (EUR/ton)",
    ))
    
    # Capacity (granularity: node_id + commodity_id)
    graph.add_node_attribute(AttributeTable(
        name="monthly_capacity",
        entity_type="node",
        attribute_class="capacity",
        granularity_keys=["node_id", "commodity_id"],
        value_columns=["min_value", "max_value"],
        value_types={"min_value": "float", "max_value": "float"},
        data=pd.DataFrame({
            "node_id": [
                "depot_naples", "depot_naples",
                "depot_florence", "depot_florence",
                "depot_bologna", "depot_bologna",
            ],
            "commodity_id": [
                "bulk_lpg", "cylinder_propane",
                "bulk_lpg", "cylinder_propane",
                "bulk_lpg", "cylinder_propane",
            ],
            "min_value": [100.0, 50.0, 80.0, 40.0, 120.0, 60.0],
            "max_value": [1000.0, 500.0, 800.0, 400.0, 1200.0, 600.0],
        }),
        description="Monthly throughput capacity (tons)",
    ))
    
    # =========================================================================
    # Edge Attributes
    # =========================================================================
    
    # Transport rates (granularity: source, target, commodity, resource)
    graph.add_edge_attribute(AttributeTable(
        name="transport_rate",
        entity_type="edge",
        attribute_class="rate",
        granularity_keys=["source_id", "target_id", "commodity_id", "resource_id"],
        value_columns=["value"],
        value_types={"value": "float"},
        data=pd.DataFrame({
            "source_id": [
                "depot_naples", "depot_naples",
                "depot_florence", "depot_florence",
            ],
            "target_id": ["zone_001", "zone_001", "zone_002", "zone_002"],
            "commodity_id": ["bulk_lpg", "cylinder_propane", "bulk_lpg", "cylinder_propane"],
            "resource_id": ["tanker", "truck_large", "tanker", "truck_large"],
            "value": [15.0, 18.0, 14.0, 17.0],
        }),
        description="Transport cost (EUR/ton)",
    ))
    
    # Trip fixed cost (granularity: source, target, resource)
    graph.add_edge_attribute(AttributeTable(
        name="trip_cost",
        entity_type="edge",
        attribute_class="cost",
        granularity_keys=["source_id", "target_id", "resource_id"],
        value_columns=["value"],
        value_types={"value": "float"},
        data=pd.DataFrame({
            "source_id": ["depot_naples", "depot_naples", "depot_florence"],
            "target_id": ["zone_001", "zone_001", "zone_002"],
            "resource_id": ["tanker", "truck_large", "tanker"],
            "value": [50.0, 40.0, 45.0],
        }),
        description="Fixed cost per trip (EUR)",
    ))
    
    # =========================================================================
    # Flows
    # =========================================================================
    
    # Primary deliveries (depot → zone)
    graph.add_flows(FlowsTable(
        name="primary_deliveries",
        granularity_keys=["source_id", "target_id", "commodity_id", "period"],
        value_column="value",
        value_type="float",
        data=pd.DataFrame({
            "source_id": [
                "depot_naples", "depot_naples", "depot_naples",
                "depot_florence", "depot_florence",
            ],
            "target_id": ["zone_001", "zone_001", "zone_002", "zone_002", "zone_003"],
            "commodity_id": ["bulk_lpg", "cylinder_propane", "bulk_lpg", "bulk_lpg", "cylinder_propane"],
            "period": ["2024-01", "2024-01", "2024-01", "2024-01", "2024-01"],
            "value": [150.0, 80.0, 120.0, 200.0, 90.0],
        }),
        description="Deliveries from depots to customer zones",
    ))
    
    # Secondary deliveries (terminal → depot)
    graph.add_flows(FlowsTable(
        name="secondary_deliveries",
        granularity_keys=["source_id", "target_id", "commodity_id", "period"],
        value_column="value",
        value_type="float",
        data=pd.DataFrame({
            "source_id": ["terminal_rome", "terminal_rome", "terminal_milan"],
            "target_id": ["depot_naples", "depot_florence", "depot_bologna"],
            "commodity_id": ["bulk_lpg", "bulk_lpg", "bulk_lng"],
            "period": ["2024-01", "2024-01", "2024-01"],
            "value": [500.0, 400.0, 600.0],
        }),
        description="Deliveries from terminals to depots",
    ))
    
    # =========================================================================
    # Demands
    # =========================================================================
    
    graph.demands = pd.DataFrame({
        "node_id": ["zone_001", "zone_001", "zone_002", "zone_003", "zone_004"],
        "commodity_id": ["bulk_lpg", "cylinder_propane", "bulk_lpg", "cylinder_propane", "bulk_lpg"],
        "period": ["2024-01", "2024-01", "2024-01", "2024-01", "2024-01"],
        "quantity": [200.0, 100.0, 150.0, 80.0, 120.0],
    })
    
    return graph


def demonstrate_queries(graph: GraphDataWithQueries) -> None:
    """Demonstrate query capabilities."""
    
    print("=" * 60)
    print("GRAPH INFO")
    print("=" * 60)
    print(graph.info())
    print()
    
    # ----- Node queries -----
    print("=" * 60)
    print("NODE QUERIES")
    print("=" * 60)
    
    print("\nDepots:")
    print(graph.get_nodes("depot"))
    
    print("\nDepots with tags (region, ownership):")
    print(graph.nodes_with_tags("region", "ownership", node_type="depot"))
    
    print("\nDepots with coordinates:")
    print(graph.nodes_with_coordinates("depot"))
    
    # ----- Attribute queries -----
    print("\n" + "=" * 60)
    print("ATTRIBUTE QUERIES")
    print("=" * 60)
    
    print("\nVariable cost attribute description:")
    print(graph.describe_attr("variable_cost"))
    
    print("\nVariable costs for bulk_lpg:")
    print(graph.get_node_attr("variable_cost", commodity_id="bulk_lpg"))
    
    print("\nDepots with fixed cost and capacity:")
    print(graph.nodes_with_attrs(
        "fixed_cost",
        "monthly_capacity",
        node_type="depot",
        commodity_id="bulk_lpg"
    ))
    
    # ----- Edge building -----
    print("\n" + "=" * 60)
    print("EDGE BUILDING")
    print("=" * 60)
    
    graph.set_distance_service(backend="haversine")
    
    print("\nEdges from depots to zones:")
    edges = graph.build_edges(
        source_types=["depot"],
        target_types=["zone"]
    )
    print(edges.head(10))
    
    print("\nEdges with transport rates:")
    edges_with_rates = graph.edges_with_attrs(
        "transport_rate",
        source_types=["depot"],
        target_types=["zone"],
        commodity_id="bulk_lpg",
    )
    print(edges_with_rates)
    
    # ----- Flow queries -----
    print("\n" + "=" * 60)
    print("FLOW QUERIES")
    print("=" * 60)
    
    print("\nPrimary deliveries in January 2024:")
    print(graph.get_flows("primary_deliveries", period="2024-01"))
    
    print("\nTotal deliveries by source depot:")
    print(graph.aggregate_flows(
        "primary_deliveries",
        group_by=["source_id"],
        period="2024-01"
    ))


def demonstrate_validation(graph: GraphDataWithQueries) -> None:
    """Demonstrate validation."""
    
    print("\n" + "=" * 60)
    print("VALIDATION")
    print("=" * 60)
    
    result = validate_graph(graph)
    print(result)


def main():
    """Run example."""
    
    # Create graph
    graph = create_logistics_graph()
    
    # Demonstrate queries
    demonstrate_queries(graph)
    
    # Validate
    demonstrate_validation(graph)
    
    # Save
    print("\n" + "=" * 60)
    print("SAVING")
    print("=" * 60)
    save_parquet(graph, "/tmp/logistics_graph")
    print("Saved to /tmp/logistics_graph/")


if __name__ == "__main__":
    main()
