"""Unit tests for ``build_model`` and the build pipeline.

Verifies that ``build_model`` takes DataLoaderMock output and produces a valid
``ResolvedModelData`` with expected resolved tables.
"""

from __future__ import annotations

import pytest

from gbp.build.pipeline import build_model
from gbp.core.model import RawModelData, ResolvedModelData
from gbp.loaders.contracts import GraphLoaderConfig
from gbp.loaders.dataloader_graph import DataLoaderGraph
from gbp.loaders.dataloader_mock import DataLoaderMock

MOCK_CONFIG: dict = {
    "n_stations": 10,
    "n_depots": 2,
    "n_timestamps": 72,
    "time_freq": "h",
    "start_date": "2025-01-01",
    "ebike_fraction": 0.3,
    "depot_capacity": 200,
    "seed": 42,
}


@pytest.fixture(scope="module")
def raw_model() -> RawModelData:
    mock_source = DataLoaderMock(MOCK_CONFIG, n_trucks=3, truck_capacity_bikes=20)
    graph_loader = DataLoaderGraph(mock_source, GraphLoaderConfig())
    return graph_loader.load()


@pytest.fixture(scope="module")
def resolved_model(raw_model) -> ResolvedModelData:
    return build_model(raw_model)


# ---------------------------------------------------------------------------
# build_model produces a ResolvedModelData
# ---------------------------------------------------------------------------


def test_build_model_returns_resolved_type(resolved_model):
    assert isinstance(resolved_model, ResolvedModelData)


# ---------------------------------------------------------------------------
# Resolved tables use period_id instead of date
# ---------------------------------------------------------------------------


def test_demand_has_period_id(resolved_model):
    """Demand table should be resolved to period_id, not raw dates."""
    demand = resolved_model.demand
    if demand.empty:
        pytest.skip("No demand in mock data")
    assert "period_id" in demand.columns, "Resolved demand must have 'period_id'"


def test_supply_has_period_id(resolved_model):
    """Supply table should be resolved to period_id."""
    supply = resolved_model.supply
    if supply.empty:
        pytest.skip("No supply in mock data")
    assert "period_id" in supply.columns, "Resolved supply must have 'period_id'"


# ---------------------------------------------------------------------------
# Periods table is populated
# ---------------------------------------------------------------------------


def test_periods_non_empty(resolved_model):
    assert not resolved_model.periods.empty, "Resolved periods must not be empty"


def test_periods_have_period_index(resolved_model):
    assert "period_index" in resolved_model.periods.columns
    assert "period_id" in resolved_model.periods.columns


def test_periods_sorted_by_index(resolved_model):
    indices = resolved_model.periods["period_index"].tolist()
    assert indices == sorted(indices), "Periods must be sorted by period_index"


# ---------------------------------------------------------------------------
# Facilities preserved
# ---------------------------------------------------------------------------


def test_facilities_preserved(raw_model, resolved_model):
    """All raw facilities should be present in the resolved model."""
    raw_ids = set(raw_model.facilities["facility_id"])
    resolved_ids = set(resolved_model.facilities["facility_id"])
    assert raw_ids == resolved_ids


# ---------------------------------------------------------------------------
# Edges built
# ---------------------------------------------------------------------------


def test_edges_non_empty(resolved_model):
    """Build pipeline should produce edges from the mock distance matrix."""
    assert not resolved_model.edges.empty, "Resolved model should have edges"


def test_edges_have_required_columns(resolved_model):
    edges = resolved_model.edges
    for col in ("source_id", "target_id", "modal_type"):
        assert col in edges.columns, f"Edges missing column: {col}"


# ---------------------------------------------------------------------------
# Inventory initial is populated
# ---------------------------------------------------------------------------


def test_inventory_initial_non_empty(resolved_model):
    assert not resolved_model.inventory_initial.empty


def test_inventory_initial_has_required_columns(resolved_model):
    inv = resolved_model.inventory_initial
    for col in ("facility_id", "commodity_category", "quantity"):
        assert col in inv.columns, f"inventory_initial missing column: {col}"


def test_inventory_initial_non_negative(resolved_model):
    inv = resolved_model.inventory_initial
    negative = inv[inv["quantity"] < -1e-9]
    assert negative.empty, f"Negative initial inventory:\n{negative.to_string()}"


# ---------------------------------------------------------------------------
# Resource fleet / resources present when trucks configured
# ---------------------------------------------------------------------------


def test_resources_or_fleet_present(resolved_model):
    """Mock config has n_trucks=3, so resource data should exist."""
    has_resources = not resolved_model.resources.empty
    has_fleet = not resolved_model.resource_fleet.empty
    assert has_resources or has_fleet, (
        "Mock with trucks should have resource_fleet or resources"
    )


# ---------------------------------------------------------------------------
# Commodity categories present
# ---------------------------------------------------------------------------


def test_commodity_categories_non_empty(resolved_model):
    assert not resolved_model.commodity_categories.empty
