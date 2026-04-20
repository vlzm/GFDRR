"""Tests for transformation resolution."""

from __future__ import annotations

import pandas as pd

from gbp.build.transformation import resolve_transformations


def test_repair_transformation_denormalized() -> None:
    """Maintenance hub repair links input and output commodities."""
    facilities = pd.DataFrame(
        {
            "facility_id": ["hub1"],
            "facility_type": ["maintenance_hub"],
            "name": ["Hub"],
        }
    )
    transformations = pd.DataFrame(
        {
            "transformation_id": ["t1"],
            "facility_id": ["hub1"],
            "operation_type": ["repair"],
            "loss_rate": [0.05],
            "batch_size": [None],
            "batch_size_unit": [None],
        }
    )
    inputs = pd.DataFrame(
        {
            "transformation_id": ["t1"],
            "commodity_category": ["broken_bike"],
            "ratio": [1.0],
        }
    )
    outputs = pd.DataFrame(
        {
            "transformation_id": ["t1"],
            "commodity_category": ["working_bike"],
            "ratio": [1.0],
        }
    )
    out = resolve_transformations(facilities, transformations, inputs, outputs)
    assert out is not None
    assert len(out) == 1
    assert out.iloc[0]["commodity_category_in"] == "broken_bike"
    assert out.iloc[0]["commodity_category_out"] == "working_bike"


def test_missing_tables_returns_none() -> None:
    """Any missing transformation table yields None."""
    facilities = pd.DataFrame({"facility_id": ["x"], "facility_type": ["depot"], "name": ["X"]})
    assert resolve_transformations(facilities, None, None, None) is None
