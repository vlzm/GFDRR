"""Unit tests for DataLoaderMock truck-fleet parameterisation.

Tests cover:
- No truck rows when n_trucks=0 (default).
- Correct row count when n_trucks=3.
- Correct capacity in df_resource_capacities.
- Full round-trip: build_model -> Environment -> state.resources.
"""

from __future__ import annotations

import pytest

from gbp.build.pipeline import build_model
from gbp.consumers.simulator.engine import Environment
from gbp.consumers.simulator.config import EnvironmentConfig
from gbp.loaders import DataLoaderGraph, DataLoaderMock, GraphLoaderConfig

# Minimal config sufficient for build_model to succeed.
_BASE_CONFIG: dict = {"n_stations": 4, "n_depots": 1, "n_timestamps": 24}


def _build_resolved(n_trucks: int, truck_capacity_bikes: int = 30):
    """Load and build a resolved model with the given truck configuration."""
    mock = DataLoaderMock(_BASE_CONFIG, n_trucks=n_trucks, truck_capacity_bikes=truck_capacity_bikes)
    loader = DataLoaderGraph(mock, GraphLoaderConfig(distance_backend="haversine"))
    return build_model(loader.load())


class TestNoTrucksWhenNTrucksZero:
    """df_resources emits zero truck rows when n_trucks=0."""

    def test_no_trucks_when_n_trucks_zero(self) -> None:
        mock = DataLoaderMock(_BASE_CONFIG, n_trucks=0)
        mock.load_data()
        if mock.df_resources.empty:
            truck_count = 0
        else:
            truck_count = int(
                mock.df_resources["resource_id"].astype(str).str.startswith("truck").sum()
            )
        assert truck_count == 0, (
            f"Expected zero truck rows in df_resources when n_trucks=0, got {truck_count}"
        )

    def test_df_resources_empty_when_n_trucks_zero(self) -> None:
        mock = DataLoaderMock(_BASE_CONFIG, n_trucks=0)
        mock.load_data()
        assert mock.df_resources.empty, "df_resources must be empty when n_trucks=0"

    def test_df_resource_capacities_empty_when_n_trucks_zero(self) -> None:
        mock = DataLoaderMock(_BASE_CONFIG, n_trucks=0)
        mock.load_data()
        assert mock.df_resource_capacities.empty, (
            "df_resource_capacities must be empty when n_trucks=0"
        )

    def test_df_truck_rates_empty_when_n_trucks_zero(self) -> None:
        mock = DataLoaderMock(_BASE_CONFIG, n_trucks=0)
        mock.load_data()
        assert mock.df_truck_rates.empty, "df_truck_rates must be empty when n_trucks=0"


class TestTrucksEmittedWhenConfigured:
    """Exact row count and column shape when n_trucks > 0."""

    def test_trucks_emitted_when_configured(self) -> None:
        mock = DataLoaderMock(_BASE_CONFIG, n_trucks=3)
        mock.load_data()
        assert len(mock.df_resources) == 3, (
            f"Expected 3 rows in df_resources, got {len(mock.df_resources)}"
        )

    def test_resource_ids_are_unique(self) -> None:
        mock = DataLoaderMock(_BASE_CONFIG, n_trucks=3)
        mock.load_data()
        assert mock.df_resources["resource_id"].nunique() == 3

    def test_df_resources_has_resource_id_column_only(self) -> None:
        """Protocol contract: df_resources exposes only resource_id."""
        mock = DataLoaderMock(_BASE_CONFIG, n_trucks=3)
        mock.load_data()
        assert set(mock.df_resources.columns) == {"resource_id"}


class TestTruckCapacityInResourceCapacities:
    """df_resource_capacities stores the configured truck_capacity_bikes."""

    def test_truck_capacity_in_resource_capacities(self) -> None:
        mock = DataLoaderMock(_BASE_CONFIG, n_trucks=2, truck_capacity_bikes=20)
        mock.load_data()
        assert len(mock.df_resource_capacities) == 2
        assert (mock.df_resource_capacities["capacity"] == 20).all(), (
            "All rows in df_resource_capacities must have capacity == truck_capacity_bikes"
        )

    def test_default_truck_capacity_is_30(self) -> None:
        mock = DataLoaderMock(_BASE_CONFIG, n_trucks=1)
        mock.load_data()
        assert mock.df_resource_capacities.iloc[0]["capacity"] == 30


class TestResolvedStateHasTrucks:
    """Full round-trip: build_model -> Environment -> state.resources."""

    def test_resolved_state_has_trucks(self) -> None:
        resolved = _build_resolved(n_trucks=3, truck_capacity_bikes=20)
        env = Environment(resolved, config=EnvironmentConfig())
        state = env.state
        truck_rows = state.resources[
            state.resources["resource_category"] == "rebalancing_truck"
        ]
        assert len(truck_rows) == 3, (
            f"Expected 3 rebalancing_truck rows in state.resources, got {len(truck_rows)}"
        )

    def test_resolved_state_no_trucks_when_zero(self) -> None:
        resolved = _build_resolved(n_trucks=0)
        env = Environment(resolved, config=EnvironmentConfig())
        state = env.state
        truck_rows = state.resources[
            state.resources["resource_category"] == "rebalancing_truck"
        ]
        assert len(truck_rows) == 0, (
            f"Expected 0 rebalancing_truck rows when n_trucks=0, got {len(truck_rows)}"
        )
