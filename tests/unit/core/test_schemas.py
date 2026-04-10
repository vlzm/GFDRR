"""Tests for Pydantic row schemas."""

from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from gbp.core.enums import FacilityRole, PeriodType
from gbp.core.schemas import (
    Demand,
    Edge,
    Facility,
    Period,
    Transformation,
    TransformationInput,
    TransformationOutput,
)


def test_facility_optional_coords() -> None:
    """Facility lat/lon are optional."""
    f = Facility(facility_id="s1", facility_type="station", name="A")
    assert f.lat is None and f.lon is None
    f2 = Facility(facility_id="s2", facility_type="station", name="B", lat=48.8, lon=2.3)
    assert f2.lat == 48.8


def test_edge_reliability_bounds() -> None:
    """Edge reliability must lie in [0, 1] when set."""
    Edge(
        source_id="a",
        target_id="b",
        modal_type="road",
        distance=1.0,
        lead_time_hours=2.0,
        reliability=0.95,
    )
    with pytest.raises(ValidationError):
        Edge(
            source_id="a",
            target_id="b",
            modal_type="road",
            distance=1.0,
            lead_time_hours=2.0,
            reliability=1.5,
        )


def test_demand_min_order_optional() -> None:
    """Demand min_order_quantity defaults to None."""
    d = Demand(
        facility_id="c1",
        commodity_category="working_bike",
        date=date(2025, 1, 1),
        quantity=10.0,
    )
    assert d.min_order_quantity is None


def test_transformation_chain() -> None:
    """Transformation inputs/outputs link by transformation_id."""
    t = Transformation(
        transformation_id="t1",
        facility_id="hub1",
        operation_type="repair",
        loss_rate=0.05,
    )
    ti = TransformationInput(transformation_id="t1", commodity_category="broken_bike", ratio=1.0)
    to = TransformationOutput(transformation_id="t1", commodity_category="working_bike", ratio=1.0)
    assert t.facility_id == "hub1"
    assert ti.ratio == to.ratio


def test_period_model() -> None:
    """Period carries segment index and PeriodType."""
    p = Period(
        period_id="p0",
        planning_horizon_id="h1",
        segment_index=0,
        period_index=0,
        period_type=PeriodType.DAY,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
    )
    assert p.period_type == PeriodType.DAY


def test_facility_role_record_uses_enum() -> None:
    """FacilityRoleRecord.role is a FacilityRole enum."""
    from gbp.core.schemas import FacilityRoleRecord

    r = FacilityRoleRecord(facility_id="s1", role=FacilityRole.SOURCE)
    assert r.role == FacilityRole.SOURCE
