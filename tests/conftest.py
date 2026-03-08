"""Shared fixtures for the test suite."""

import pytest
import pandas as pd

from gbp.graph import GraphDataWithQueries, AttributeTable, FlowsTable
from gbp.loaders import DataLoaderMock, DataLoaderGraph, GraphLoaderConfig


# =============================================================================
# Graph Model fixtures (notebook 01 data)
# =============================================================================


@pytest.fixture()
def sample_graph() -> GraphDataWithQueries:
    """Full graph similar to the one built in notebook 01."""
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

    resources = pd.DataFrame({
        "id": ["truck_large", "truck_small", "tanker"],
        "resource_type": ["vehicle", "vehicle", "vehicle"],
    })

    commodities = pd.DataFrame({
        "id": ["bulk_lpg", "bulk_lng", "cylinder_propane", "cylinder_mix"],
        "commodity_type": ["bulk", "bulk", "cylinder", "cylinder"],
    })

    coordinates = pd.DataFrame({
        "node_id": [
            "terminal_rome", "terminal_milan",
            "depot_naples", "depot_florence", "depot_bologna",
            "zone_001", "zone_002", "zone_003", "zone_004",
        ],
        "latitude": [41.9, 45.5, 40.8, 43.8, 44.5, 41.0, 42.5, 44.0, 45.0],
        "longitude": [12.5, 9.2, 14.2, 11.2, 11.3, 14.5, 12.0, 11.0, 10.0],
    })

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

    graph = GraphDataWithQueries(
        nodes=nodes,
        resources=resources,
        commodities=commodities,
        coordinates=coordinates,
        tags=tags,
    )

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
    ))

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
    ))

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
        date_column="period",
        time_granularity="monthly",
        date_format="%Y-%m",
    ))

    graph.add_edge_attribute(AttributeTable(
        name="transport_rate",
        entity_type="edge",
        attribute_class="rate",
        granularity_keys=["source_id", "target_id", "commodity_id", "resource_id"],
        value_columns=["value"],
        value_types={"value": "float"},
        data=pd.DataFrame({
            "source_id": ["depot_naples", "depot_naples", "depot_florence", "depot_florence"],
            "target_id": ["zone_001", "zone_001", "zone_002", "zone_002"],
            "commodity_id": ["bulk_lpg", "cylinder_propane", "bulk_lpg", "cylinder_propane"],
            "resource_id": ["tanker", "truck_large", "tanker", "truck_large"],
            "value": [15.0, 18.0, 14.0, 17.0],
        }),
    ))

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
            "commodity_id": [
                "bulk_lpg", "cylinder_propane", "bulk_lpg",
                "bulk_lpg", "cylinder_propane",
            ],
            "period": ["2024-01"] * 5,
            "value": [150.0, 80.0, 120.0, 200.0, 90.0],
        }),
    ))

    graph.demands = pd.DataFrame({
        "node_id": ["zone_001", "zone_001", "zone_002", "zone_003", "zone_004"],
        "commodity_id": [
            "bulk_lpg", "cylinder_propane", "bulk_lpg",
            "cylinder_propane", "bulk_lpg",
        ],
        "period": ["2024-01"] * 5,
        "quantity": [200.0, 100.0, 150.0, 80.0, 120.0],
    })

    return graph


# =============================================================================
# Graph Loader fixtures (notebook 02 data)
# =============================================================================


@pytest.fixture()
def mock_config() -> dict:
    return {"n": 8, "n_depots": 2, "n_timestamps": 48}


@pytest.fixture()
def loaded_graph_loader(mock_config: dict) -> DataLoaderGraph:
    """DataLoaderGraph with data already loaded."""
    mock = DataLoaderMock(mock_config)
    loader = DataLoaderGraph(mock, GraphLoaderConfig(distance_backend="haversine"))
    loader.load_data()
    return loader
