"""Tests for spine assembly."""

from __future__ import annotations

from datetime import date

import pandas as pd

from gbp.build.pipeline import build_model
from gbp.build.spine import assemble_spines
from gbp.core.attributes.defaults import get_structural_attribute_specs
from gbp.core.enums import AttributeKind
from tests.unit.build.fixtures import minimal_raw_model


def test_assemble_spines_uses_defaults_when_none() -> None:
    """Passing ``attribute_specs=None`` applies the bike-sharing catalog."""
    raw = minimal_raw_model(with_edges=True)
    resolved = build_model(raw)
    spines = assemble_spines(resolved, attribute_specs=None)
    assert "facility" in spines and "edge" in spines and "resource" in spines
    assert spines["resource"], "resource spines should include base_capacity group"


def test_assemble_spines_skips_missing_tables() -> None:
    """Nullable attribute sources that are absent do not fail assembly."""
    raw = minimal_raw_model(with_edges=True)
    resolved = build_model(raw)
    spines = assemble_spines(resolved, get_structural_attribute_specs())
    assert isinstance(spines["facility"], dict)


def test_pipeline_attaches_spines() -> None:
    """``build_model`` attaches spine dicts to ``ResolvedModelData``."""
    raw = minimal_raw_model(with_edges=True)
    resolved = build_model(raw)
    assert resolved.edge_spines is not None
    assert resolved.resource_spines is not None


def test_assemble_spines_with_operation_and_transport_costs() -> None:
    """Facility and edge spines pick up resolved cost tables via registry."""
    raw = minimal_raw_model(with_edges=True)
    raw.attributes.register(
        name="operation_cost",
        data=pd.DataFrame({
            "facility_id": ["d1"],
            "operation_type": ["storage"],
            "commodity_category": ["working_bike"],
            "date": [date(2025, 1, 1)],
            "cost_per_unit": [0.5],
            "cost_unit": ["usd"],
        }),
        entity_type="facility",
        kind=AttributeKind.COST,
        grain=("facility_id", "operation_type", "commodity_category", "date"),
        value_column="cost_per_unit",
        aggregation="mean",
    )
    raw.attributes.register(
        name="transport_cost",
        data=pd.DataFrame({
            "source_id": ["d1"],
            "target_id": ["s1"],
            "modal_type": ["road"],
            "resource_category": ["rebalancing_truck"],
            "date": [date(2025, 1, 1)],
            "cost_per_unit": [2.0],
            "cost_unit": ["usd"],
        }),
        entity_type="edge",
        kind=AttributeKind.COST,
        grain=("source_id", "target_id", "modal_type", "resource_category", "date"),
        value_column="cost_per_unit",
        aggregation="mean",
    )
    resolved = build_model(raw)
    spines = assemble_spines(resolved)
    assert spines["edge"]
    esp = next(iter(spines["edge"].values()))
    assert "transport_cost" in esp.columns
    fsp = next(iter(spines["facility"].values()))
    assert "operation_cost" in fsp.columns


def test_empty_edge_spines_when_resolved_edges_absent() -> None:
    """If ``resolved.edges`` is missing or empty, edge spine assembly is skipped."""
    raw = minimal_raw_model(with_edges=True)
    resolved = build_model(raw)
    resolved.edges = None
    spines = assemble_spines(resolved)
    assert spines["edge"] == {}
