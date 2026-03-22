"""Tests for ``make_raw_model`` quick-start factory."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from gbp.core.factory import make_raw_model
from gbp.core.model import RawModelData


def _minimal_facilities() -> pd.DataFrame:
    return pd.DataFrame({
        "facility_id": ["d1", "s1", "s2"],
        "facility_type": ["depot", "station", "station"],
        "name": ["Depot", "Station 1", "Station 2"],
    })


def _minimal_commodity_categories() -> pd.DataFrame:
    return pd.DataFrame({
        "commodity_category_id": ["bike"],
        "name": ["Bike"],
        "unit": ["unit"],
    })


def _minimal_resource_categories() -> pd.DataFrame:
    return pd.DataFrame({
        "resource_category_id": ["truck"],
        "name": ["Truck"],
        "base_capacity": [20.0],
        "capacity_unit": ["unit"],
    })


class TestMakeRawModelMinimal:
    """Minimal 3-entity call produces a valid RawModelData."""

    def test_returns_raw_model_data(self) -> None:
        raw = make_raw_model(
            facilities=_minimal_facilities(),
            commodity_categories=_minimal_commodity_categories(),
            resource_categories=_minimal_resource_categories(),
            planning_start=date(2025, 1, 1),
            planning_end=date(2025, 1, 8),
        )
        assert isinstance(raw, RawModelData)

    def test_temporal_tables_generated(self) -> None:
        raw = make_raw_model(
            facilities=_minimal_facilities(),
            commodity_categories=_minimal_commodity_categories(),
            resource_categories=_minimal_resource_categories(),
            planning_start=date(2025, 1, 1),
            planning_end=date(2025, 1, 8),
        )
        assert raw.planning_horizon is not None
        assert len(raw.planning_horizon) == 1
        assert raw.periods is not None
        assert len(raw.periods) == 7

    def test_facility_roles_derived(self) -> None:
        raw = make_raw_model(
            facilities=_minimal_facilities(),
            commodity_categories=_minimal_commodity_categories(),
            resource_categories=_minimal_resource_categories(),
            planning_start=date(2025, 1, 1),
            planning_end=date(2025, 1, 4),
        )
        assert raw.facility_roles is not None
        assert not raw.facility_roles.empty
        depot_roles = set(
            raw.facility_roles[raw.facility_roles["facility_id"] == "d1"]["role"]
        )
        assert "storage" in depot_roles
        assert "transshipment" in depot_roles

    def test_facility_operations_generated(self) -> None:
        raw = make_raw_model(
            facilities=_minimal_facilities(),
            commodity_categories=_minimal_commodity_categories(),
            resource_categories=_minimal_resource_categories(),
            planning_start=date(2025, 1, 1),
            planning_end=date(2025, 1, 4),
        )
        assert raw.facility_operations is not None
        assert (raw.facility_operations["enabled"]).all()

    def test_default_edge_rules(self) -> None:
        raw = make_raw_model(
            facilities=_minimal_facilities(),
            commodity_categories=_minimal_commodity_categories(),
            resource_categories=_minimal_resource_categories(),
            planning_start=date(2025, 1, 1),
            planning_end=date(2025, 1, 4),
        )
        assert raw.edge_rules is not None
        assert len(raw.edge_rules) == 1
        assert raw.edge_rules["modal_type"].iloc[0] == "road"


class TestMakeRawModelCustom:
    """Factory with custom edge_rules and extra tables."""

    def test_custom_edge_rules(self) -> None:
        custom_rules = pd.DataFrame({
            "source_type": ["depot"],
            "target_type": ["station"],
            "commodity_category": ["bike"],
            "modal_type": ["road"],
            "enabled": [True],
        })
        raw = make_raw_model(
            facilities=_minimal_facilities(),
            commodity_categories=_minimal_commodity_categories(),
            resource_categories=_minimal_resource_categories(),
            planning_start=date(2025, 1, 1),
            planning_end=date(2025, 1, 4),
            edge_rules=custom_rules,
        )
        assert len(raw.edge_rules) == 1
        assert raw.edge_rules["source_type"].iloc[0] == "depot"

    def test_extra_tables_passed_through(self) -> None:
        inv = pd.DataFrame({
            "facility_id": ["s1"],
            "commodity_category": ["bike"],
            "quantity": [10.0],
            "quantity_unit": ["unit"],
        })
        raw = make_raw_model(
            facilities=_minimal_facilities(),
            commodity_categories=_minimal_commodity_categories(),
            resource_categories=_minimal_resource_categories(),
            planning_start=date(2025, 1, 1),
            planning_end=date(2025, 1, 4),
            inventory_initial=inv,
        )
        assert raw.inventory_initial is not None
        assert len(raw.inventory_initial) == 1

    def test_demand_and_supply_args(self) -> None:
        demand = pd.DataFrame({
            "facility_id": ["s1"],
            "commodity_category": ["bike"],
            "date": [date(2025, 1, 1)],
            "quantity": [5.0],
            "quantity_unit": ["unit"],
        })
        raw = make_raw_model(
            facilities=_minimal_facilities(),
            commodity_categories=_minimal_commodity_categories(),
            resource_categories=_minimal_resource_categories(),
            planning_start=date(2025, 1, 1),
            planning_end=date(2025, 1, 4),
            demand=demand,
        )
        assert raw.demand is not None
        assert len(raw.demand) == 1


class TestMakeRawModelValidation:
    """Factory validates the assembled model."""

    def test_invalid_facilities_raises(self) -> None:
        bad_facilities = pd.DataFrame({
            "facility_id": ["s1"],
            "facility_type": ["station"],
        })
        with pytest.raises(ValueError, match="missing required columns"):
            make_raw_model(
                facilities=bad_facilities,
                commodity_categories=_minimal_commodity_categories(),
                resource_categories=_minimal_resource_categories(),
                planning_start=date(2025, 1, 1),
                planning_end=date(2025, 1, 4),
            )

    def test_week_period_type(self) -> None:
        raw = make_raw_model(
            facilities=_minimal_facilities(),
            commodity_categories=_minimal_commodity_categories(),
            resource_categories=_minimal_resource_categories(),
            planning_start=date(2025, 1, 1),
            planning_end=date(2025, 2, 1),
            period_type="week",
        )
        assert raw.periods is not None
        assert (raw.periods["period_type"] == "week").all()
