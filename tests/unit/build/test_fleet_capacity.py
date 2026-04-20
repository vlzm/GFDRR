"""Tests for fleet capacity computation."""

from __future__ import annotations

import pandas as pd

from gbp.build.fleet_capacity import compute_fleet_capacity


def test_aggregate_count_times_base_capacity() -> None:
    """Without L3 resources, effective capacity is count * base_capacity."""
    fleet = pd.DataFrame(
        {
            "facility_id": ["d1"],
            "resource_category": ["truck"],
            "count": [3],
        }
    )
    rc = pd.DataFrame(
        {
            "resource_category_id": ["truck"],
            "name": ["Truck"],
            "base_capacity": [10.0],
            "capacity_unit": ["bike"],
        }
    )
    out = compute_fleet_capacity(fleet, rc, resources=None)
    assert out is not None
    assert out.iloc[0]["effective_capacity"] == 30.0


def test_none_fleet_returns_none() -> None:
    """Missing fleet table returns None."""
    rc = pd.DataFrame(
        {
            "resource_category_id": ["truck"],
            "name": ["Truck"],
            "base_capacity": [10.0],
            "capacity_unit": ["bike"],
        }
    )
    assert compute_fleet_capacity(None, rc, None) is None
