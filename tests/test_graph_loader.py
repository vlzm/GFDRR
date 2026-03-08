"""Tests for the graph data loader (gbp.loaders.DataLoaderGraph).

Covers: loading, snapshot structure, static parts, temporal inventory,
        validation, distance service, config options.
"""

import pytest
import pandas as pd

from gbp.graph import GraphData, validate_graph
from gbp.loaders import DataLoaderMock, DataLoaderGraph, GraphLoaderConfig


N_STATIONS = 8
N_DEPOTS = 2
N_TOTAL = N_STATIONS + N_DEPOTS


# =============================================================================
# Loading & snapshot
# =============================================================================


class TestLoading:

    def test_load_data_succeeds(self, loaded_graph_loader):
        assert loaded_graph_loader.available_dates is not None
        assert len(loaded_graph_loader.available_dates) == 48

    def test_snapshot_returns_graph_data(self, loaded_graph_loader):
        date = loaded_graph_loader.available_dates[0]
        snap = loaded_graph_loader.get_snapshot(date)
        assert isinstance(snap, GraphData)

    def test_snapshot_repr(self, loaded_graph_loader):
        snap = loaded_graph_loader.get_snapshot(loaded_graph_loader.available_dates[0])
        r = repr(snap)
        assert "GraphData" in r
        assert f"nodes={N_TOTAL}" in r


# =============================================================================
# Static parts
# =============================================================================


class TestStaticParts:

    def test_nodes(self, loaded_graph_loader):
        snap = loaded_graph_loader.get_snapshot(loaded_graph_loader.available_dates[0])
        assert len(snap.nodes) == N_TOTAL
        assert "id" in snap.nodes.columns
        assert "node_type" in snap.nodes.columns
        types = set(snap.nodes["node_type"])
        assert types == {"station", "depot"}

    def test_station_count(self, loaded_graph_loader):
        snap = loaded_graph_loader.get_snapshot(loaded_graph_loader.available_dates[0])
        stations = snap.nodes[snap.nodes["node_type"] == "station"]
        assert len(stations) == N_STATIONS

    def test_depot_count(self, loaded_graph_loader):
        snap = loaded_graph_loader.get_snapshot(loaded_graph_loader.available_dates[0])
        depots = snap.nodes[snap.nodes["node_type"] == "depot"]
        assert len(depots) == N_DEPOTS

    def test_coordinates(self, loaded_graph_loader):
        snap = loaded_graph_loader.get_snapshot(loaded_graph_loader.available_dates[0])
        assert snap.coordinates is not None
        assert len(snap.coordinates) == N_TOTAL
        assert {"node_id", "latitude", "longitude"}.issubset(snap.coordinates.columns)

    def test_coordinate_ranges(self, loaded_graph_loader):
        snap = loaded_graph_loader.get_snapshot(loaded_graph_loader.available_dates[0])
        assert (snap.coordinates["latitude"].between(-90, 90)).all()
        assert (snap.coordinates["longitude"].between(-180, 180)).all()

    def test_resources(self, loaded_graph_loader):
        snap = loaded_graph_loader.get_snapshot(loaded_graph_loader.available_dates[0])
        assert snap.resources is not None
        assert "id" in snap.resources.columns
        assert "resource_type" in snap.resources.columns
        assert "capacity" in snap.resources.columns
        assert (snap.resources["resource_type"] == "vehicle").all()

    def test_commodities(self, loaded_graph_loader):
        snap = loaded_graph_loader.get_snapshot(loaded_graph_loader.available_dates[0])
        assert snap.commodities is not None
        assert len(snap.commodities) == 1
        assert snap.commodities.iloc[0]["id"] == "bike"

    def test_edges_fully_connected(self, loaded_graph_loader):
        snap = loaded_graph_loader.get_snapshot(loaded_graph_loader.available_dates[0])
        assert snap.edges is not None
        expected_edges = N_TOTAL * (N_TOTAL - 1)
        assert len(snap.edges) == expected_edges
        assert "source_id" in snap.edges.columns
        assert "distance_km" in snap.edges.columns

    def test_edge_distances_positive(self, loaded_graph_loader):
        snap = loaded_graph_loader.get_snapshot(loaded_graph_loader.available_dates[0])
        assert (snap.edges["distance_km"] > 0).all()


# =============================================================================
# Node attributes
# =============================================================================


class TestNodeAttributes:

    def test_inventory_capacity_attribute_exists(self, loaded_graph_loader):
        snap = loaded_graph_loader.get_snapshot(loaded_graph_loader.available_dates[0])
        assert "inventory_capacity" in snap.node_attributes

    def test_inventory_capacity_structure(self, loaded_graph_loader):
        snap = loaded_graph_loader.get_snapshot(loaded_graph_loader.available_dates[0])
        attr = snap.node_attributes["inventory_capacity"]
        assert attr.entity_type == "node"
        assert attr.attribute_class == "capacity"
        assert "node_id" in attr.granularity_keys
        assert "value" in attr.value_columns
        assert len(attr.data) == N_STATIONS

    def test_inventory_capacity_values_positive(self, loaded_graph_loader):
        snap = loaded_graph_loader.get_snapshot(loaded_graph_loader.available_dates[0])
        attr = snap.node_attributes["inventory_capacity"]
        assert (attr.data["value"] > 0).all()


# =============================================================================
# Inventory (temporal)
# =============================================================================


class TestInventory:

    def test_inventory_present(self, loaded_graph_loader):
        snap = loaded_graph_loader.get_snapshot(loaded_graph_loader.available_dates[0])
        assert snap.inventory is not None

    def test_inventory_columns(self, loaded_graph_loader):
        snap = loaded_graph_loader.get_snapshot(loaded_graph_loader.available_dates[0])
        assert {"node_id", "commodity_id", "quantity"}.issubset(snap.inventory.columns)

    def test_inventory_station_count(self, loaded_graph_loader):
        snap = loaded_graph_loader.get_snapshot(loaded_graph_loader.available_dates[0])
        assert len(snap.inventory) == N_STATIONS

    def test_inventory_quantities_non_negative(self, loaded_graph_loader):
        snap = loaded_graph_loader.get_snapshot(loaded_graph_loader.available_dates[0])
        assert (snap.inventory["quantity"] >= 0).all()

    def test_different_snapshots_may_differ(self, loaded_graph_loader):
        dates = loaded_graph_loader.available_dates
        snap_first = loaded_graph_loader.get_snapshot(dates[0])
        snap_last = loaded_graph_loader.get_snapshot(dates[-1])
        q_first = int(snap_first.inventory["quantity"].sum())
        q_last = int(snap_last.inventory["quantity"].sum())
        assert q_first >= 0
        assert q_last >= 0

    def test_inventory_timeseries_shape(self, loaded_graph_loader):
        ts = loaded_graph_loader.inventory_timeseries
        assert ts.shape == (48, N_STATIONS)


# =============================================================================
# Validation
# =============================================================================


class TestValidation:

    def test_snapshot_passes_validation(self, loaded_graph_loader):
        snap = loaded_graph_loader.get_snapshot(loaded_graph_loader.available_dates[0])
        result = validate_graph(snap)
        assert result.is_valid
        assert result.error_count == 0


# =============================================================================
# Distance service
# =============================================================================


class TestDistanceService:

    def test_distance_service_attached(self, loaded_graph_loader):
        snap = loaded_graph_loader.get_snapshot(loaded_graph_loader.available_dates[0])
        assert snap.distance_service is not None

    def test_distance_positive(self, loaded_graph_loader):
        snap = loaded_graph_loader.get_snapshot(loaded_graph_loader.available_dates[0])
        ids = list(snap.nodes["id"][:2])
        d = snap.distance_service.get_distance(ids[0], ids[1])
        assert d > 0

    def test_distance_symmetric(self, loaded_graph_loader):
        snap = loaded_graph_loader.get_snapshot(loaded_graph_loader.available_dates[0])
        ids = list(snap.nodes["id"][:2])
        d_ab = snap.distance_service.get_distance(ids[0], ids[1])
        d_ba = snap.distance_service.get_distance(ids[1], ids[0])
        assert abs(d_ab - d_ba) < 1e-6


# =============================================================================
# Config options
# =============================================================================


class TestConfig:

    def test_build_edges_false(self, mock_config):
        mock = DataLoaderMock(mock_config)
        loader = DataLoaderGraph(mock, GraphLoaderConfig(build_edges=False))
        loader.load_data()
        snap = loader.get_snapshot(loader.available_dates[0])
        assert snap.edges is None
        assert snap.distance_service is None

    def test_default_config(self, mock_config):
        mock = DataLoaderMock(mock_config)
        loader = DataLoaderGraph(mock)
        loader.load_data()
        snap = loader.get_snapshot(loader.available_dates[0])
        assert snap.edges is not None

    def test_euclidean_backend(self, mock_config):
        mock = DataLoaderMock(mock_config)
        loader = DataLoaderGraph(mock, GraphLoaderConfig(distance_backend="euclidean"))
        loader.load_data()
        snap = loader.get_snapshot(loader.available_dates[0])
        assert snap.edges is not None
        assert (snap.edges["distance_km"] > 0).all()
