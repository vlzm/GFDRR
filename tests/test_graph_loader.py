"""Tests for ``DataLoaderGraph`` → ``RawModelData`` / ``ResolvedModelData``."""

from __future__ import annotations

from gbp.build.pipeline import build_model
from gbp.core.enums import ModalType
from gbp.core.model import RawModelData, ResolvedModelData
from gbp.loaders import DataLoaderGraph, DataLoaderMock, GraphLoaderConfig
from gbp.loaders.dataloader_graph import COMMODITY_CATEGORIES, RESOURCE_CATEGORY

N_STATIONS = 8
N_DEPOTS = 2
N_TOTAL = N_STATIONS + N_DEPOTS
N_CATEGORIES = len(COMMODITY_CATEGORIES)


# =============================================================================
# Loading
# =============================================================================


class TestLoading:
    def test_load_data_succeeds(self, loaded_graph_loader: DataLoaderGraph) -> None:
        assert loaded_graph_loader.available_dates is not None
        assert len(loaded_graph_loader.available_dates) == 48

    def test_raw_and_resolved_types(
        self,
        loaded_graph_loader: DataLoaderGraph,
        resolved_graph_model: ResolvedModelData,
    ) -> None:
        assert isinstance(loaded_graph_loader.raw, RawModelData)
        assert isinstance(resolved_graph_model, ResolvedModelData)


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
        cc_ids = set(raw.commodity_categories["commodity_category_id"])
        assert cc_ids == set(COMMODITY_CATEGORIES)
        assert RESOURCE_CATEGORY in raw.resource_categories["resource_category_id"].values

    def test_periods_align_with_mock_horizon(
        self,
        loaded_graph_loader: DataLoaderGraph,
        resolved_graph_model: ResolvedModelData,
    ) -> None:
        # The loader no longer emits ``periods`` directly — build_model derives
        # them from the horizon segments.  ``raw.periods`` must stay absent,
        # and the resolved grid must be daily.
        raw = loaded_graph_loader.raw
        assert raw.periods is None
        assert resolved_graph_model.periods is not None and not resolved_graph_model.periods.empty
        assert resolved_graph_model.periods["period_type"].iloc[0] == "day"
        assert "periods" in resolved_graph_model.build_report.derivations


# =============================================================================
# Resolved model (edges, build pipeline)
# =============================================================================


class TestResolvedModel:
    def test_edges_fully_connected_when_enabled(
        self, resolved_graph_model: ResolvedModelData,
    ) -> None:
        res = resolved_graph_model
        assert res.edges is not None
        expected = N_TOTAL * (N_TOTAL - 1)
        assert len(res.edges) == expected
        assert (res.edges["modal_type"] == ModalType.ROAD.value).all()
        assert (res.edges["distance"] > 0).all()

    def test_edge_commodities(self, resolved_graph_model: ResolvedModelData) -> None:
        res = resolved_graph_model
        assert res.edge_commodities is not None
        assert not res.edge_commodities.empty
        cc_in_edges = set(res.edge_commodities["commodity_category"])
        assert cc_in_edges == set(COMMODITY_CATEGORIES)


# =============================================================================
# Inventory time series
# =============================================================================


class TestSourceInventoryWide:
    def test_inventory_multiindex_shape(self, loaded_graph_loader: DataLoaderGraph) -> None:
        ts = loaded_graph_loader.source.df_inventory_ts
        assert ts.shape == (48, N_TOTAL * N_CATEGORIES)
        assert ts.columns.names == ["facility_id", "commodity_category"]

    def test_different_timestamps_may_differ(self, loaded_graph_loader: DataLoaderGraph) -> None:
        ts = loaded_graph_loader.source.df_inventory_ts
        a = ts.iloc[0].sum()
        b = ts.iloc[-1].sum()
        assert a >= 0
        assert b >= 0


class TestModelValidation:
    def test_raw_validate(self, loaded_graph_loader: DataLoaderGraph) -> None:
        loaded_graph_loader.raw.validate()

    def test_resolved_has_expected_tables(
        self, resolved_graph_model: ResolvedModelData,
    ) -> None:
        """``ResolvedModelData.validate`` expects raw date columns; time-resolved tables use ``period_id``."""
        res = resolved_graph_model
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
        resolved = build_model(loader.load())
        assert resolved.edges is None or resolved.edges.empty

    def test_default_config_has_edges(self, mock_config: dict) -> None:
        mock = DataLoaderMock(mock_config)
        loader = DataLoaderGraph(mock)
        resolved = build_model(loader.load())
        assert resolved.edges is not None
        assert not resolved.edges.empty

    def test_euclidean_backend(self, mock_config: dict) -> None:
        mock = DataLoaderMock(mock_config)
        loader = DataLoaderGraph(mock, GraphLoaderConfig(distance_backend="euclidean"))
        resolved = build_model(loader.load())
        assert (resolved.edges["distance"] > 0).all()


# =============================================================================
# Citi Bike-like mock extras
# =============================================================================


class TestMockExtras:
    def test_stations_have_minimal_columns(self, mock_config: dict) -> None:
        mock = DataLoaderMock(mock_config)
        mock.load_data()
        assert set(mock.df_stations.columns) == {"station_id", "lat", "lon"}

    def test_station_capacities_have_commodity_category(self, mock_config: dict) -> None:
        mock = DataLoaderMock(mock_config)
        mock.load_data()
        assert {"station_id", "commodity_category", "capacity"}.issubset(
            mock.df_station_capacities.columns,
        )
        cc_vals = set(mock.df_station_capacities["commodity_category"])
        assert cc_vals == set(COMMODITY_CATEGORIES)

    def test_resources_without_capacity(self, mock_config: dict) -> None:
        mock = DataLoaderMock(mock_config)
        mock.load_data()
        assert set(mock.df_resources.columns) == {"resource_id"}
        assert {"resource_id", "capacity"}.issubset(mock.df_resource_capacities.columns)

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
        station_ids = set(mock.df_stations["station_id"])
        assert set(mock.df_trips["start_station_id"]).issubset(station_ids)
        assert set(mock.df_trips["end_station_id"]).issubset(station_ids)

    def test_trips_rideable_type_matches_commodities(self, mock_config: dict) -> None:
        mock = DataLoaderMock(mock_config)
        mock.load_data()
        types = set(mock.df_trips["rideable_type"])
        assert types.issubset(set(COMMODITY_CATEGORIES))

    def test_cost_tables_exist(self, mock_config: dict) -> None:
        mock = DataLoaderMock(mock_config)
        mock.load_data()
        assert {"station_id", "fixed_cost_station"}.issubset(mock.df_station_costs.columns)
        assert {"node_id", "fixed_cost_depot"}.issubset(mock.df_depot_costs.columns)
        assert {
            "resource_id",
            "cost_per_km",
            "cost_per_hour",
            "fixed_dispatch_cost",
        }.issubset(mock.df_truck_rates.columns)


class TestSourceExposed:
    def test_stations_minimal_columns(self, loaded_graph_loader: DataLoaderGraph) -> None:
        assert set(loaded_graph_loader.source.df_stations.columns) == {"station_id", "lat", "lon"}

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
            "date", "quantity",
        }
        assert expected.issubset(raw.observed_flow.columns)

    def test_observed_flow_has_both_categories(self, loaded_graph_loader: DataLoaderGraph) -> None:
        raw = loaded_graph_loader.raw
        assert raw.observed_flow is not None
        cc_in_flow = set(raw.observed_flow["commodity_category"])
        assert cc_in_flow == set(COMMODITY_CATEGORIES)

    def test_observed_inventory_populated(self, loaded_graph_loader: DataLoaderGraph) -> None:
        raw = loaded_graph_loader.raw
        assert raw.observed_inventory is not None
        assert not raw.observed_inventory.empty
        expected = {
            "facility_id", "commodity_category",
            "date", "quantity",
        }
        assert expected.issubset(raw.observed_inventory.columns)

    def test_observed_inventory_has_both_categories(self, loaded_graph_loader: DataLoaderGraph) -> None:
        raw = loaded_graph_loader.raw
        assert raw.observed_inventory is not None
        cc_in_inv = set(raw.observed_inventory["commodity_category"])
        assert cc_in_inv == set(COMMODITY_CATEGORIES)

    def test_demand_derived_from_observations(
        self,
        loaded_graph_loader: DataLoaderGraph,
        resolved_graph_model: ResolvedModelData,
    ) -> None:
        # Derivation now happens in build_model, not the loader — demand
        # appears on the resolved model, not the raw model.
        raw = loaded_graph_loader.raw
        assert raw.demand is None
        assert resolved_graph_model.demand is not None and not resolved_graph_model.demand.empty
        assert "demand" in resolved_graph_model.build_report.derivations

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
        raw = loader.load()
        assert raw.observed_flow is None
        assert raw.observed_inventory is None

    def test_resolved_observations_have_period_id(
        self, resolved_graph_model: ResolvedModelData,
    ) -> None:
        res = resolved_graph_model
        if res.observed_flow is not None and not res.observed_flow.empty:
            assert "period_id" in res.observed_flow.columns
            assert "date" not in res.observed_flow.columns
        if res.observed_inventory is not None and not res.observed_inventory.empty:
            assert "period_id" in res.observed_inventory.columns
            assert "date" not in res.observed_inventory.columns


class TestCoreTableAccess:
    def test_operation_capacities_per_commodity(self, loaded_graph_loader: DataLoaderGraph) -> None:
        assert "operation_capacity" in loaded_graph_loader.raw.attributes
        oc = loaded_graph_loader.raw.attributes.get("operation_capacity").data
        assert len(oc) == N_TOTAL * N_CATEGORIES
        assert (oc["operation_type"] == "storage").all()
        cc_vals = set(oc["commodity_category"])
        assert cc_vals == set(COMMODITY_CATEGORIES)

    def test_inventory_initial_per_commodity(self, loaded_graph_loader: DataLoaderGraph) -> None:
        raw = loaded_graph_loader.raw
        inv = raw.inventory_initial
        assert inv is not None
        assert len(inv) == N_TOTAL * N_CATEGORIES
        ts0 = loaded_graph_loader.source.df_inventory_ts.iloc[0]
        for _, row in inv.iterrows():
            fid = str(row["facility_id"])
            cc = str(row["commodity_category"])
            assert int(row["quantity"]) == int(ts0[(fid, cc)])
