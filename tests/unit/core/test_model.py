"""Tests for ``RawModelData`` and ``ResolvedModelData``."""

from __future__ import annotations

from dataclasses import fields
from datetime import date

import pandas as pd
import pytest

from gbp.core.enums import FacilityRole, PeriodType
from gbp.core.model import RawModelData, ResolvedModelData


def _minimal_raw_frames() -> dict[str, pd.DataFrame]:
    return {
        "facilities": pd.DataFrame(
            {
                "facility_id": ["s1", "d1"],
                "facility_type": ["station", "depot"],
                "name": ["Station 1", "Depot 1"],
            }
        ),
        "commodity_categories": pd.DataFrame(
            {
                "commodity_category_id": ["working_bike"],
                "name": ["Working bike"],
                "unit": ["bike"],
            }
        ),
        "resource_categories": pd.DataFrame(
            {
                "resource_category_id": ["rebalancing_truck"],
                "name": ["Rebalancing truck"],
                "base_capacity": [20.0],
                "capacity_unit": ["bike"],
            }
        ),
        "planning_horizon": pd.DataFrame(
            {
                "planning_horizon_id": ["h1"],
                "name": ["Week1"],
                "start_date": [date(2025, 1, 1)],
                "end_date": [date(2025, 1, 8)],
            }
        ),
        "planning_horizon_segments": pd.DataFrame(
            {
                "planning_horizon_id": ["h1"],
                "segment_index": [0],
                "start_date": [date(2025, 1, 1)],
                "end_date": [date(2025, 1, 8)],
                "period_type": [PeriodType.DAY.value],
            }
        ),
        "periods": pd.DataFrame(
            {
                "period_id": ["p0"],
                "planning_horizon_id": ["h1"],
                "segment_index": [0],
                "period_index": [0],
                "period_type": [PeriodType.DAY.value],
                "start_date": [date(2025, 1, 1)],
                "end_date": [date(2025, 1, 2)],
            }
        ),
        "facility_roles": pd.DataFrame(
            {
                "facility_id": ["s1", "s1", "s1"],
                "role": [
                    FacilityRole.SOURCE.value,
                    FacilityRole.SINK.value,
                    FacilityRole.STORAGE.value,
                ],
            }
        ),
        "facility_operations": pd.DataFrame(
            {
                "facility_id": ["s1", "s1", "s1"],
                "operation_type": ["receiving", "storage", "dispatch"],
                "enabled": [True, True, True],
            }
        ),
        "edge_rules": pd.DataFrame(
            {
                "source_type": ["depot"],
                "target_type": ["station"],
                "commodity_category": ["working_bike"],
                "modal_type": ["road"],
                "enabled": [True],
            }
        ),
    }


def test_raw_model_data_validate_ok() -> None:
    """Minimal bike-sharing frames pass column validation."""
    d = _minimal_raw_frames()
    raw = RawModelData(
        facilities=d["facilities"],
        commodity_categories=d["commodity_categories"],
        resource_categories=d["resource_categories"],
        planning_horizon=d["planning_horizon"],
        planning_horizon_segments=d["planning_horizon_segments"],
        periods=d["periods"],
        facility_roles=d["facility_roles"],
        facility_operations=d["facility_operations"],
        edge_rules=d["edge_rules"],
    )
    raw.validate()


def test_raw_model_data_validate_missing_column() -> None:
    """Validation fails when a required schema column is absent."""
    d = _minimal_raw_frames()
    bad = d["facilities"].drop(columns=["name"])
    with pytest.raises(ValueError, match="missing required columns"):
        RawModelData(
            facilities=bad,
            commodity_categories=d["commodity_categories"],
            resource_categories=d["resource_categories"],
            planning_horizon=d["planning_horizon"],
            planning_horizon_segments=d["planning_horizon_segments"],
            periods=d["periods"],
            facility_roles=d["facility_roles"],
            facility_operations=d["facility_operations"],
            edge_rules=d["edge_rules"],
        ).validate()


def test_raw_groups_cover_all_fields() -> None:
    """_GROUPS must reference every non-underscore dataclass field in RawModelData."""
    all_group_fields = {f for names in RawModelData._GROUPS.values() for f in names}
    dataclass_fields = {f.name for f in fields(RawModelData) if not f.name.startswith("_")}
    assert all_group_fields == dataclass_fields


def test_resolved_groups_cover_all_dataframe_fields() -> None:
    """_GROUPS must reference every DataFrame field in ResolvedModelData.

    Spine fields (dict[str, DataFrame]) are excluded — they are handled
    by the separate ``spine_tables`` property.
    """
    all_group_fields = {f for names in ResolvedModelData._GROUPS.values() for f in names}
    spine_fields = {"facility_spines", "edge_spines", "resource_spines"}
    dataclass_fields = {
        f.name for f in fields(ResolvedModelData)
        if not f.name.startswith("_") and f.name not in spine_fields
    }
    assert all_group_fields == dataclass_fields


def test_resolved_model_data_edge_lead_time_optional() -> None:
    """Resolved model allows optional generated edge_lead_time_resolved table."""
    d = _minimal_raw_frames()
    resolved = ResolvedModelData(
        facilities=d["facilities"],
        commodity_categories=d["commodity_categories"],
        resource_categories=d["resource_categories"],
        planning_horizon=d["planning_horizon"],
        planning_horizon_segments=d["planning_horizon_segments"],
        periods=d["periods"],
        facility_roles=d["facility_roles"],
        facility_operations=d["facility_operations"],
        edge_rules=d["edge_rules"],
        edge_lead_time_resolved=None,
    )
    resolved.validate()
