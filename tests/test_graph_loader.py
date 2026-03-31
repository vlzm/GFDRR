"""Tests for ``DataLoaderGraph`` → ``RawModelData`` / ``ResolvedModelData``."""

from __future__ import annotations

import pandas as pd

from gbp.core.enums import ModalType
from gbp.core.model import RawModelData, ResolvedModelData
from gbp.loaders import DataLoaderGraph, DataLoaderMock, GraphLoaderConfig
from gbp.loaders.dataloader_graph import COMMODITY_CATEGORY, RESOURCE_CATEGORY

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
# Inventory time series
# =============================================================================


class TestSourceInventoryWide:
    def test_inventory_wide_matrix_shape(self, loaded_graph_loader: DataLoaderGraph) -> None:
        ts = loaded_graph_loader.source.df_inventory_ts
        assert ts.shape == (48, N_STATIONS)

    def test_different_timestamps_may_differ(self, loaded_graph_loader: DataLoaderGraph) -> None:
        ts = loaded_graph_loader.source.df_inventory_ts
        a = ts.iloc[0].sum()
        b = ts.iloc[-1].sum()
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


class TestSourceExposed:
    def test_region_id_on_stations(self, loaded_graph_loader: DataLoaderGraph) -> None:
        assert "region_id" in loaded_graph_loader.source.df_stations.columns

    def test_telemetry_on_source(self, loaded_graph_loader: DataLoaderGraph) -> None:
        assert loaded_graph_loader.source.df_telemetry_ts is not None
        assert len(loaded_graph_loader.available_dates) == 48


# =============================================================================
# Access to core tables
# =============================================================================


class TestObservations:
    """Observed flow and inventory built from trips/telemetry."""

    def test_observed_flow_populated(self, loaded_graph_loader: DataLoaderGraph) -> None:
        raw = loaded_graph_loader.raw
        assert raw.observed_flow is not None
        assert not raw.observed_flow.empty
        expected = {
            "source_id", "target_id", "commodity_category",
            "date", "quantity", "quantity_unit",
        }
        assert expected.issubset(raw.observed_flow.columns)

    def test_observed_inventory_populated(self, loaded_graph_loader: DataLoaderGraph) -> None:
        raw = loaded_graph_loader.raw
        assert raw.observed_inventory is not None
        assert not raw.observed_inventory.empty
        expected = {
            "facility_id", "commodity_category",
            "date", "quantity", "quantity_unit",
        }
        assert expected.issubset(raw.observed_inventory.columns)

    def test_demand_derived_from_observations(self, loaded_graph_loader: DataLoaderGraph) -> None:
        raw = loaded_graph_loader.raw
        assert raw.demand is not None
        assert not raw.demand.empty

    def test_observed_flow_references_known_facilities(
        self, loaded_graph_loader: DataLoaderGraph,
    ) -> None:
        raw = loaded_graph_loader.raw
        known = set(raw.facilities["facility_id"])
        assert raw.observed_flow is not None
        assert set(raw.observed_flow["source_id"]).issubset(known)
        assert set(raw.observed_flow["target_id"]).issubset(known)

    def test_build_observations_false(self, mock_config: dict) -> None:
        mock = DataLoaderMock(mock_config)
        loader = DataLoaderGraph(mock, GraphLoaderConfig(build_observations=False))
        loader.load_data()
        assert loader.raw.observed_flow is None
        assert loader.raw.observed_inventory is None

    def test_resolved_observations_have_period_id(
        self, loaded_graph_loader: DataLoaderGraph,
    ) -> None:
        res = loaded_graph_loader.resolved
        if res.observed_flow is not None and not res.observed_flow.empty:
            assert "period_id" in res.observed_flow.columns
            assert "date" not in res.observed_flow.columns
        if res.observed_inventory is not None and not res.observed_inventory.empty:
            assert "period_id" in res.observed_inventory.columns
            assert "date" not in res.observed_inventory.columns


class TestCoreTableAccess:
    def test_operation_capacities_for_stations(self, loaded_graph_loader: DataLoaderGraph) -> None:
        assert "operation_capacity" in loaded_graph_loader.raw.attributes
        oc = loaded_graph_loader.raw.attributes.get("operation_capacity").data
        assert len(oc) == N_STATIONS
        assert (oc["operation_type"] == "storage").all()

    def test_inventory_initial_matches_first_timestep(self, loaded_graph_loader: DataLoaderGraph) -> None:
        raw = loaded_graph_loader.raw
        inv = raw.inventory_initial
        assert inv is not None
        assert len(inv) == N_STATIONS
        ts0 = loaded_graph_loader.source.df_inventory_ts.iloc[0]
        for _, row in inv.iterrows():
            fid = str(row["facility_id"])
            assert int(row["quantity"]) == int(ts0[fid])
