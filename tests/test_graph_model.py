"""Tests for the universal graph model (gbp.graph).

Covers: GraphData, GraphDataWithQueries, AttributeTable, FlowsTable,
        EdgeBuilder, DistanceService, GraphValidator.
"""

import pytest
import pandas as pd

from gbp.graph import (
    AttributeTable,
    FlowsTable,
    GraphDataWithQueries,
    GRANULARITY_TO_FREQ,
    validate_graph,
)


# =============================================================================
# GraphData — construction & basic properties
# =============================================================================


class TestGraphDataConstruction:

    def test_minimal_graph(self):
        graph = GraphDataWithQueries(
            nodes=pd.DataFrame({"id": ["a"], "node_type": ["x"]}),
        )
        assert len(graph.nodes) == 1
        assert graph.edges is None
        assert graph.resources is None

    def test_full_graph_repr(self, sample_graph):
        r = repr(sample_graph)
        assert "GraphData" in r
        assert "nodes=9" in r

    def test_node_ids(self, sample_graph):
        assert "depot_naples" in sample_graph.node_ids
        assert len(sample_graph.node_ids) == 9

    def test_node_types(self, sample_graph):
        types = sample_graph.node_types
        assert set(types) == {"terminal", "depot", "zone"}

    def test_duplicate_ids_rejected(self):
        with pytest.raises(ValueError, match="duplicate"):
            GraphDataWithQueries(
                nodes=pd.DataFrame({"id": ["a", "a"], "node_type": ["x", "y"]}),
            )

    def test_missing_columns_rejected(self):
        with pytest.raises(ValueError, match="missing"):
            GraphDataWithQueries(
                nodes=pd.DataFrame({"name": ["a"]}),
            )


# =============================================================================
# Node queries
# =============================================================================


class TestNodeQueries:

    def test_get_nodes_all(self, sample_graph):
        assert len(sample_graph.get_nodes()) == 9

    def test_get_nodes_by_type(self, sample_graph):
        depots = sample_graph.get_nodes("depot")
        assert len(depots) == 3
        assert set(depots["node_type"]) == {"depot"}

    def test_get_nodes_by_ids(self, sample_graph):
        result = sample_graph.get_nodes_by_ids(["depot_naples", "zone_001"])
        assert len(result) == 2

    def test_nodes_with_coordinates(self, sample_graph):
        df = sample_graph.nodes_with_coordinates("depot")
        assert "latitude" in df.columns
        assert "longitude" in df.columns
        assert len(df) == 3


# =============================================================================
# AttributeTable — static & temporal
# =============================================================================


class TestAttributeTable:

    def test_static_attribute(self):
        attr = AttributeTable(
            name="weight_limit",
            entity_type="node",
            attribute_class="capacity",
            granularity_keys=["node_id"],
            value_columns=["value"],
            value_types={"value": "float"},
            data=pd.DataFrame({
                "node_id": ["depot_1", "depot_2"],
                "value": [1000.0, 800.0],
            }),
        )
        assert not attr.is_temporal
        assert attr.pandas_freq is None

    def test_temporal_attribute_monthly(self):
        attr = AttributeTable(
            name="cogs",
            entity_type="node",
            attribute_class="rate",
            granularity_keys=["node_id", "period"],
            value_columns=["value"],
            value_types={"value": "float"},
            data=pd.DataFrame({
                "node_id": ["n1", "n1"],
                "period": ["2024-01", "2024-02"],
                "value": [100.0, 105.0],
            }),
            date_column="period",
            time_granularity="monthly",
            date_format="%Y-%m",
        )
        assert attr.is_temporal
        assert attr.pandas_freq == "M"

    def test_temporal_attribute_hourly(self):
        attr = AttributeTable(
            name="hourly",
            entity_type="node",
            attribute_class="rate",
            granularity_keys=["node_id", "ts"],
            value_columns=["v"],
            value_types={"v": "float"},
            data=pd.DataFrame({
                "node_id": ["n1"],
                "ts": ["2024-01-01 00:00"],
                "v": [1.0],
            }),
            date_column="ts",
            time_granularity="hourly",
        )
        assert attr.pandas_freq == "h"

    def test_dimension_keys(self):
        attr = AttributeTable(
            name="vc",
            entity_type="node",
            attribute_class="cost",
            granularity_keys=["node_id", "commodity_id"],
            value_columns=["value"],
            value_types={"value": "float"},
            data=pd.DataFrame({
                "node_id": ["n1"],
                "commodity_id": ["c1"],
                "value": [10.0],
            }),
        )
        assert attr.dimension_keys == ["commodity_id"]

    def test_validation_date_column_not_in_keys(self):
        with pytest.raises(ValueError, match="must be listed in granularity_keys"):
            AttributeTable(
                name="bad",
                entity_type="node",
                attribute_class="cost",
                granularity_keys=["node_id"],
                value_columns=["value"],
                value_types={"value": "float"},
                data=pd.DataFrame({"node_id": ["n1"], "value": [1.0]}),
                date_column="period",
                time_granularity="monthly",
            )

    def test_validation_date_column_without_granularity(self):
        with pytest.raises(ValueError, match="must both be set or both be None"):
            AttributeTable(
                name="bad2",
                entity_type="node",
                attribute_class="cost",
                granularity_keys=["node_id"],
                value_columns=["value"],
                value_types={"value": "float"},
                data=pd.DataFrame({"node_id": ["n1"], "value": [1.0]}),
                date_column="period",
            )

    def test_validation_missing_column(self):
        with pytest.raises(ValueError, match="not found in data"):
            AttributeTable(
                name="bad3",
                entity_type="node",
                attribute_class="cost",
                granularity_keys=["node_id"],
                value_columns=["missing_col"],
                value_types={"missing_col": "float"},
                data=pd.DataFrame({"node_id": ["n1"]}),
            )

    def test_validation_node_needs_node_id(self):
        with pytest.raises(ValueError, match="node_id"):
            AttributeTable(
                name="bad4",
                entity_type="node",
                attribute_class="cost",
                granularity_keys=["some_key"],
                value_columns=["value"],
                value_types={"value": "float"},
                data=pd.DataFrame({"some_key": ["k1"], "value": [1.0]}),
            )

    def test_granularity_to_freq_mapping(self):
        expected = {"hourly", "daily", "weekly", "monthly", "quarterly", "yearly"}
        assert set(GRANULARITY_TO_FREQ.keys()) == expected


# =============================================================================
# Attribute queries via graph
# =============================================================================


class TestAttributeQueries:

    def test_get_node_attr(self, sample_graph):
        df = sample_graph.get_node_attr("fixed_cost")
        assert len(df) == 3
        assert "value" in df.columns

    def test_get_node_attr_with_filter(self, sample_graph):
        df = sample_graph.get_node_attr("variable_cost", commodity_id="bulk_lpg")
        assert len(df) == 3
        assert (df["commodity_id"] == "bulk_lpg").all()

    def test_get_node_attr_missing_raises(self, sample_graph):
        with pytest.raises(KeyError, match="not found"):
            sample_graph.get_node_attr("nonexistent")

    def test_list_node_attrs(self, sample_graph):
        all_attrs = sample_graph.list_node_attrs()
        assert "fixed_cost" in all_attrs
        assert "variable_cost" in all_attrs
        assert "cogs_rate" in all_attrs

    def test_list_node_attrs_by_class(self, sample_graph):
        costs = sample_graph.list_node_attrs("cost")
        assert "fixed_cost" in costs
        assert "variable_cost" in costs
        assert "cogs_rate" not in costs

    def test_nodes_with_attrs(self, sample_graph):
        df = sample_graph.nodes_with_attrs(
            "fixed_cost",
            node_type="depot",
        )
        assert len(df) == 3
        assert "value" in df.columns

    def test_describe_attr(self, sample_graph):
        desc = sample_graph.describe_attr("variable_cost")
        assert "variable_cost" in desc
        assert "cost" in desc


# =============================================================================
# Tags
# =============================================================================


class TestTagQueries:

    def test_get_tag(self, sample_graph):
        assert sample_graph.get_tag("node", "depot_naples", "region") == "south"

    def test_get_tag_missing(self, sample_graph):
        assert sample_graph.get_tag("node", "depot_naples", "nonexistent") is None

    def test_get_tags_for(self, sample_graph):
        tags = sample_graph.get_tags_for("resource", "truck_large")
        assert tags == {"fuel_type": "diesel"}

    def test_nodes_with_tags(self, sample_graph):
        df = sample_graph.nodes_with_tags("region", node_type="depot")
        assert "region" in df.columns
        assert len(df) == 3


# =============================================================================
# Edge building & distance
# =============================================================================


class TestEdgeBuilding:

    def test_build_edges(self, sample_graph):
        sample_graph.set_distance_service(backend="haversine")
        edges = sample_graph.build_edges(
            source_types=["depot"],
            target_types=["zone"],
        )
        assert len(edges) == 12  # 3 depots × 4 zones
        assert "distance_km" in edges.columns
        assert "duration_h" in edges.columns
        assert (edges["distance_km"] > 0).all()

    def test_edges_stored_on_graph(self, sample_graph):
        sample_graph.set_distance_service(backend="haversine")
        sample_graph.build_edges(source_types=["depot"], target_types=["zone"])
        assert sample_graph.edges is not None
        assert len(sample_graph.edges) == 12

    def test_edges_with_attrs(self, sample_graph):
        sample_graph.set_distance_service(backend="haversine")
        df = sample_graph.edges_with_attrs(
            "transport_rate",
            source_types=["depot"],
            target_types=["zone"],
            commodity_id="bulk_lpg",
        )
        assert len(df) == 12
        has_rate = df["value"].notna()
        assert has_rate.sum() >= 2

    def test_distance_service_without_coordinates_raises(self):
        graph = GraphDataWithQueries(
            nodes=pd.DataFrame({"id": ["a"], "node_type": ["x"]}),
        )
        with pytest.raises(ValueError, match="coordinates"):
            graph.set_distance_service()


# =============================================================================
# Flows
# =============================================================================


class TestFlows:

    def test_flows_table_creation(self):
        ft = FlowsTable(
            name="deliveries",
            granularity_keys=["source_id", "target_id", "period"],
            value_column="value",
            data=pd.DataFrame({
                "source_id": ["a"],
                "target_id": ["b"],
                "period": ["2024-01"],
                "value": [100.0],
            }),
        )
        assert len(ft) == 1
        assert ft.dimension_keys == ["period"]

    def test_flows_table_missing_source_id(self):
        with pytest.raises(ValueError, match="source_id"):
            FlowsTable(
                name="bad",
                granularity_keys=["target_id"],
                value_column="value",
            )

    def test_get_flows(self, sample_graph):
        df = sample_graph.get_flows("primary_deliveries", period="2024-01")
        assert len(df) == 5

    def test_get_flows_missing_raises(self, sample_graph):
        with pytest.raises(KeyError, match="not found"):
            sample_graph.get_flows("nonexistent")

    def test_aggregate_flows(self, sample_graph):
        df = sample_graph.aggregate_flows(
            "primary_deliveries",
            group_by=["source_id"],
            period="2024-01",
        )
        assert len(df) == 2
        assert "value" in df.columns


# =============================================================================
# Validation
# =============================================================================


class TestValidation:

    def test_valid_graph_passes(self, sample_graph):
        result = validate_graph(sample_graph)
        assert result.is_valid
        assert result.error_count == 0

    def test_info_output(self, sample_graph):
        info = sample_graph.info()
        assert "Nodes: 9" in info
        assert "Resources: 3" in info
        assert "Commodities: 4" in info

    def test_referential_integrity_bad_coord(self):
        graph = GraphDataWithQueries(
            nodes=pd.DataFrame({"id": ["a"], "node_type": ["x"]}),
        )
        result = validate_graph(graph)
        assert result.is_valid

    def test_validate_raises_on_error(self):
        graph = GraphDataWithQueries(
            nodes=pd.DataFrame({"id": ["a"], "node_type": ["x"]}),
            inventory=pd.DataFrame({"bad_col": [1]}),
        )
        result = validate_graph(graph)
        assert not result.is_valid
        with pytest.raises(ValueError, match="validation failed"):
            result.raise_if_invalid()
