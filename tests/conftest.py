"""Shared fixtures for the test suite."""

import pytest

from gbp.loaders import DataLoaderMock, DataLoaderGraph, GraphLoaderConfig


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
