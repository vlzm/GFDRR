"""Tests for ``DataLoaderGraph`` → ``RawModelData`` / ``ResolvedModelData`` / rebalancer snapshot."""

from __future__ import annotations

import pandas as pd

from gbp.core.enums import ModalType
from gbp.core.model import RawModelData, ResolvedModelData
from gbp.loaders import DataLoaderGraph, DataLoaderMock, GraphLoaderConfig
from gbp.loaders.dataloader_graph import (
    COMMODITY_CATEGORY,
    RESOURCE_CATEGORY,
    RebalancerGraphSnapshot,
    telemetry_long_from_source,
)

N_STATIONS = 8
N_DEPOTS = 2
N_TOTAL = N_STATIONS + N_DEPOTS


# =============================================================================
# Loading
# =============================================================================


class TestLoading:
    def test_load_data_succeeds(self, loaded_graph_loader: DataLoaderGraph) -> None:
        assert loaded_graph_loader.available_dates is not None
        assert len(loaded_graph_loader.available_dates) == 48

    def test_raw_and_resolved_types(self, loaded_graph_loader: DataLoaderGraph) -> None:
        assert isinstance(loaded_graph_loader.raw, RawModelData)
        assert isinstance(loaded_graph_loader.resolved, ResolvedModelData)

    def test_rebalancer_snapshot_type(self, loaded_graph_loader: DataLoaderGraph) -> None:
        date = loaded_graph_loader.available_dates[0]
        snap = loaded_graph_loader.rebalancer_snapshot(date)
        assert isinstance(snap, RebalancerGraphSnapshot)


# =============================================================================
# Raw model
# =============================================================================


class TestRawModel:
    def test_facility_count_and_types(self, loaded_graph_loader: DataLoaderGraph) -> None:
        raw = loaded_graph_loader.raw
        assert len(raw.facilities) == N_TOTAL
        types = set(raw.facilities["facility_type"])
        assert types == {"station", "depot"}

    def test_commodity_and_resource_categories(self, loaded_graph_loader: DataLoaderGraph) -> None:
        raw = loaded_graph_loader.raw
        assert COMMODITY_CATEGORY in raw.commodity_categories["commodity_category_id"].values
        assert RESOURCE_CATEGORY in raw.resource_categories["resource_category_id"].values

    def test_periods_align_with_mock_horizon(self, loaded_graph_loader: DataLoaderGraph) -> None:
        raw = loaded_graph_loader.raw
        assert not raw.periods.empty
        assert raw.periods["period_type"].iloc[0] == "day"


# =============================================================================
# Resolved model (edges, build pipeline)
# =============================================================================


class TestResolvedModel:
    def test_edges_fully_connected_when_enabled(self, loaded_graph_loader: DataLoaderGraph) -> None:
        res = loaded_graph_loader.resolved
        assert res.edges is not None
        expected = N_TOTAL * (N_TOTAL - 1)
        assert len(res.edges) == expected
        assert (res.edges["modal_type"] == ModalType.ROAD.value).all()
        assert (res.edges["distance"] > 0).all()

    def test_edge_commodities(self, loaded_graph_loader: DataLoaderGraph) -> None:
        res = loaded_graph_loader.resolved
        assert res.edge_commodities is not None
        assert not res.edge_commodities.empty
        assert (res.edge_commodities["commodity_category"] == COMMODITY_CATEGORY).all()


# =============================================================================
# Rebalancer snapshot (legacy PDP inputs)
# =============================================================================


class TestRebalancerSnapshot:
    def test_nodes_and_coordinates(self, loaded_graph_loader: DataLoaderGraph) -> None:
        snap = loaded_graph_loader.rebalancer_snapshot(loaded_graph_loader.available_dates[0])
        assert len(snap.nodes) == N_TOTAL
        assert set(snap.nodes["node_type"]) == {"station", "depot"}
        assert len(snap.coordinates) == N_TOTAL
        assert {"node_id", "latitude", "longitude"}.issubset(snap.coordinates.columns)

    def test_resources_and_rates(self, loaded_graph_loader: DataLoaderGraph) -> None:
        snap = loaded_graph_loader.rebalancer_snapshot(loaded_graph_loader.available_dates[0])
        assert "id" in snap.resources.columns
        assert "capacity" in snap.resources.columns
        assert {
            "cost_per_km",
            "cost_per_hour",
            "fixed_dispatch_cost",
        }.issubset(snap.resources.columns)

    def test_inventory_shape(self, loaded_graph_loader: DataLoaderGraph) -> None:
        snap = loaded_graph_loader.rebalancer_snapshot(loaded_graph_loader.available_dates[0])
        assert {"node_id", "commodity_id", "quantity"}.issubset(snap.inventory.columns)
        assert len(snap.inventory) == N_STATIONS
        assert (snap.inventory["quantity"] >= 0).all()

    def test_distance_lookup(self, loaded_graph_loader: DataLoaderGraph) -> None:
        snap = loaded_graph_loader.rebalancer_snapshot(loaded_graph_loader.available_dates[0])
        assert snap.distance_service is not None
        ids = list(snap.nodes["id"][:2])
        d = snap.distance_service.get_distance(ids[0], ids[1])
        assert d > 0
        d_ba = snap.distance_service.get_distance(ids[1], ids[0])
        assert abs(d - d_ba) < 1e-3


# =============================================================================
# Node attributes on snapshot
# =============================================================================


class TestSnapshotNodeAttributes:
    def test_inventory_capacity_attribute(self, loaded_graph_loader: DataLoaderGraph) -> None:
        snap = loaded_graph_loader.rebalancer_snapshot(loaded_graph_loader.available_dates[0])
        assert "inventory_capacity" in snap.node_attributes
        attr = snap.node_attributes["inventory_capacity"]
        assert attr.entity_type == "node"
        assert "node_id" in attr.granularity_keys
        assert len(attr.data) == N_STATIONS
        assert (attr.data["value"] > 0).all()

    def test_cost_and_station_info(self, loaded_graph_loader: DataLoaderGraph) -> None:
        snap = loaded_graph_loader.rebalancer_snapshot(loaded_graph_loader.available_dates[0])
        assert "station_fixed_cost" in snap.node_attributes
        assert "station_variable_cost" in snap.node_attributes
        assert "station_info" in snap.node_attributes


# =============================================================================
# Inventory time series
# =============================================================================


class TestInventoryTimeseries:
    def test_inventory_timeseries_shape(self, loaded_graph_loader: DataLoaderGraph) -> None:
        ts = loaded_graph_loader.inventory_timeseries
        assert ts.shape == (48, N_STATIONS)

    def test_different_snapshots_may_differ(self, loaded_graph_loader: DataLoaderGraph) -> None:
        dates = loaded_graph_loader.available_dates
        a = loaded_graph_loader.rebalancer_snapshot(dates[0]).inventory["quantity"].sum()
        b = loaded_graph_loader.rebalancer_snapshot(dates[-1]).inventory["quantity"].sum()
        assert a >= 0
        assert b >= 0


class TestModelValidation:
    def test_raw_validate(self, loaded_graph_loader: DataLoaderGraph) -> None:
        loaded_graph_loader.raw.validate()

    def test_resolved_has_expected_tables(self, loaded_graph_loader: DataLoaderGraph) -> None:
        """``ResolvedModelData.validate`` expects raw date columns; time-resolved tables use ``period_id``."""
        res = loaded_graph_loader.resolved
        assert res.facilities is not None
        assert res.edges is not None and not res.edges.empty
        assert res.edge_lead_time_resolved is not None


# =============================================================================
# Config
# =============================================================================


class TestConfig:
    def test_build_edges_false(self, mock_config: dict) -> None:
        mock = DataLoaderMock(mock_config)
        loader = DataLoaderGraph(mock, GraphLoaderConfig(build_edges=False))
        loader.load_data()
        assert loader.resolved.edges is None or loader.resolved.edges.empty

    def test_default_config_has_edges(self, mock_config: dict) -> None:
        mock = DataLoaderMock(mock_config)
        loader = DataLoaderGraph(mock)
        loader.load_data()
        assert loader.resolved.edges is not None
        assert not loader.resolved.edges.empty

    def test_euclidean_backend(self, mock_config: dict) -> None:
        mock = DataLoaderMock(mock_config)
        loader = DataLoaderGraph(mock, GraphLoaderConfig(distance_backend="euclidean"))
        loader.load_data()
        assert (loader.resolved.edges["distance"] > 0).all()


# =============================================================================
# Citi Bike-like mock extras
# =============================================================================


class TestMockExtras:
    def test_stations_include_citibike_metadata(self, mock_config: dict) -> None:
        mock = DataLoaderMock(mock_config)
        mock.load_data()
        assert {
            "station_id",
            "name",
            "short_name",
            "region_id",
            "capacity",
            "is_installed",
            "is_renting",
            "is_returning",
        }.issubset(mock.df_stations.columns)

    def test_telemetry_columns_exist(self, mock_config: dict) -> None:
        mock = DataLoaderMock(mock_config)
        mock.load_data()
        expected_cols = {
            "timestamp",
            "region_id",
            "lat",
            "lon",
            "station_id",
            "capacity",
            "short_name",
            "name",
            "num_docks_available",
            "is_returning",
            "last_reported",
            "num_bikes_available",
            "is_installed",
            "is_renting",
            "num_ebikes_available",
            "num_docks_disabled",
            "num_bikes_disabled",
        }
        assert expected_cols.issubset(mock.df_telemetry_ts.columns)
        assert len(mock.df_telemetry_ts) == len(mock.timestamps) * N_STATIONS

    def test_trips_schema_and_station_references(self, mock_config: dict) -> None:
        mock = DataLoaderMock(mock_config)
        mock.load_data()
        expected_cols = {
            "ride_id",
            "rideable_type",
            "started_at",
            "ended_at",
            "start_station_name",
            "start_station_id",
            "end_station_name",
            "end_station_id",
            "start_lat",
            "start_lng",
            "end_lat",
            "end_lng",
            "member_casual",
        }
        assert expected_cols.issubset(mock.df_trips.columns)
        station_ids = set(mock.df_stations["node_id"])
        assert set(mock.df_trips["start_station_id"]).issubset(station_ids)
        assert set(mock.df_trips["end_station_id"]).issubset(station_ids)

    def test_cost_tables_exist(self, mock_config: dict) -> None:
        mock = DataLoaderMock(mock_config)
        mock.load_data()
        assert {
            "station_id",
            "fixed_cost_per_visit",
            "cost_per_bike_moved",
        }.issubset(mock.df_station_costs.columns)
        assert {
            "resource_id",
            "cost_per_km",
            "cost_per_hour",
            "fixed_dispatch_cost",
        }.issubset(mock.df_truck_rates.columns)


# =============================================================================
# Enrichment helpers (telemetry, trips, tags)
# =============================================================================


class TestEnrichmentHelpers:
    def test_telemetry_long_shape(self, loaded_graph_loader: DataLoaderGraph) -> None:
        long_df = telemetry_long_from_source(loaded_graph_loader.telemetry_ts)
        assert {"node_id", "metric", "timestamp", "value"}.issubset(long_df.columns)
        assert {"num_bikes_available", "num_ebikes_available"}.issubset(set(long_df["metric"]))

    def test_trip_flows_hourly(self, loaded_graph_loader: DataLoaderGraph) -> None:
        tf = loaded_graph_loader.trip_flows_hourly
        assert {"source_id", "target_id", "period", "value"}.issubset(tf.columns)

    def test_region_tags_from_source(self, loaded_graph_loader: DataLoaderGraph) -> None:
        st = loaded_graph_loader._source.df_stations
        assert "region_id" in st.columns

    def test_loader_exposes_telemetry_and_timestamps(self, loaded_graph_loader: DataLoaderGraph) -> None:
        assert loaded_graph_loader.telemetry_ts is not None
        assert len(loaded_graph_loader.available_dates) == 48


# =============================================================================
# Access to core tables
# =============================================================================


class TestCoreTableAccess:
    def test_operation_capacities_for_stations(self, loaded_graph_loader: DataLoaderGraph) -> None:
        oc = loaded_graph_loader.raw.operation_capacities
        assert oc is not None
        assert len(oc) == N_STATIONS
        assert (oc["operation_type"] == "storage").all()

    def test_inventory_initial_matches_first_timestep(self, loaded_graph_loader: DataLoaderGraph) -> None:
        raw = loaded_graph_loader.raw
        inv = raw.inventory_initial
        assert inv is not None
        assert len(inv) == N_STATIONS
        ts0 = loaded_graph_loader.inventory_timeseries.iloc[0]
        for _, row in inv.iterrows():
            fid = str(row["facility_id"])
            assert int(row["quantity"]) == int(ts0[fid])
