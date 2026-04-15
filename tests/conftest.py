"""Shared fixtures for the test suite."""

import pytest

from gbp.build.pipeline import build_model
from gbp.core.model import ResolvedModelData
from gbp.loaders import DataLoaderGraph, DataLoaderMock, GraphLoaderConfig


# =============================================================================
# Graph Loader fixtures (notebook 02 data)
# =============================================================================


@pytest.fixture()
def mock_config() -> dict:
    return {"n_stations": 8, "n_depots": 2, "n_timestamps": 48}


@pytest.fixture()
def loaded_graph_loader(mock_config: dict) -> DataLoaderGraph:
    """DataLoaderGraph with the raw model assembled (no build yet)."""
    mock = DataLoaderMock(mock_config)
    loader = DataLoaderGraph(mock, GraphLoaderConfig(distance_backend="haversine"))
    loader.load()
    return loader


@pytest.fixture()
def resolved_graph_model(loaded_graph_loader: DataLoaderGraph) -> ResolvedModelData:
    """ResolvedModelData built from the loaded graph loader's raw."""
    return build_model(loaded_graph_loader.raw)
