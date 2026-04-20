"""Tests for ``gbp.build.defaults`` derivation helpers."""

from __future__ import annotations

import datetime as dt

import pandas as pd

from gbp.build.defaults import (
    DEFAULT_COMMODITY_CATEGORY_ID,
    DEFAULT_RESOURCE_CATEGORY_ID,
    default_commodity_categories,
    default_resource_categories,
    derive_demand_from_flow,
    derive_facility_roles,
    derive_inventory_initial,
    derive_supply_from_flow,
)


# ---------------------------------------------------------------------------
# derive_facility_roles
# ---------------------------------------------------------------------------


class TestDeriveFacilityRoles:
    def test_station_with_full_operations_has_source_sink_storage(self) -> None:
        facilities = pd.DataFrame({
            "facility_id": ["s1"],
            "facility_type": ["station"],
        })
        ops = pd.DataFrame({
            "facility_id": ["s1", "s1", "s1"],
            "operation_type": ["receiving", "storage", "dispatch"],
            "enabled": [True, True, True],
        })
        roles = derive_facility_roles(facilities, ops)
        assert set(roles.loc[roles["facility_id"] == "s1", "role"]) >= {
            "source", "sink", "storage",
        }

    def test_station_without_storage_op_drops_storage_role(self) -> None:
        facilities = pd.DataFrame({
            "facility_id": ["s1"],
            "facility_type": ["station"],
        })
        ops = pd.DataFrame({
            "facility_id": ["s1", "s1"],
            "operation_type": ["receiving", "dispatch"],
            "enabled": [True, True],
        })
        roles = derive_facility_roles(facilities, ops)
        station_roles = set(roles.loc[roles["facility_id"] == "s1", "role"])
        assert "storage" not in station_roles

    def test_facility_with_no_operations_uses_type_defaults(self) -> None:
        facilities = pd.DataFrame({
            "facility_id": ["d1"],
            "facility_type": ["depot"],
        })
        ops = pd.DataFrame(columns=["facility_id", "operation_type", "enabled"])
        roles = derive_facility_roles(facilities, ops)
        # Empty operations set strips storage role; transshipment remains for depot
        assert "transshipment" in set(roles["role"])


# ---------------------------------------------------------------------------
# default_commodity_categories / default_resource_categories
# ---------------------------------------------------------------------------


def test_default_commodity_categories_has_single_sentinel_row() -> None:
    df = default_commodity_categories()
    assert len(df) == 1
    assert df["commodity_category_id"].iloc[0] == DEFAULT_COMMODITY_CATEGORY_ID


def test_default_resource_categories_has_single_sentinel_row() -> None:
    df = default_resource_categories()
    assert len(df) == 1
    assert df["resource_category_id"].iloc[0] == DEFAULT_RESOURCE_CATEGORY_ID


# ---------------------------------------------------------------------------
# derive_demand_from_flow / derive_supply_from_flow
# ---------------------------------------------------------------------------


def _flow_fixture() -> pd.DataFrame:
    return pd.DataFrame({
        "source_id": ["a", "a", "b"],
        "target_id": ["b", "b", "a"],
        "commodity_category": ["bike"] * 3,
        "date": [dt.date(2025, 1, 1), dt.date(2025, 1, 1), dt.date(2025, 1, 1)],
        "quantity": [1.0, 2.0, 4.0],
        "resource_id": [None, None, None],
    })


def test_derive_demand_groups_by_source_and_date() -> None:
    demand = derive_demand_from_flow(_flow_fixture())
    a_row = demand.loc[(demand["facility_id"] == "a"), "quantity"].sum()
    assert a_row == 3.0


def test_derive_supply_groups_by_target_and_date() -> None:
    supply = derive_supply_from_flow(_flow_fixture())
    b_row = supply.loc[(supply["facility_id"] == "b"), "quantity"].sum()
    assert b_row == 3.0


def test_derivation_ignores_resource_assigned_flow() -> None:
    flow = _flow_fixture()
    flow.loc[0, "resource_id"] = "truck_1"  # operator move — must be excluded
    demand = derive_demand_from_flow(flow)
    a_row = demand.loc[(demand["facility_id"] == "a"), "quantity"].sum()
    assert a_row == 2.0  # only the second organic trip counts


def test_derive_demand_empty_input_returns_empty_schema() -> None:
    demand = derive_demand_from_flow(pd.DataFrame())
    assert demand.empty
    assert list(demand.columns) == ["facility_id", "commodity_category", "date", "quantity"]


# ---------------------------------------------------------------------------
# derive_inventory_initial
# ---------------------------------------------------------------------------


def test_derive_inventory_initial_picks_earliest_snapshot() -> None:
    observed = pd.DataFrame({
        "facility_id": ["s1", "s1", "s2"],
        "commodity_category": ["bike"] * 3,
        "date": [dt.date(2025, 1, 2), dt.date(2025, 1, 1), dt.date(2025, 1, 3)],
        "quantity": [7.0, 5.0, 10.0],
    })
    inv = derive_inventory_initial(observed)
    s1_qty = inv.loc[inv["facility_id"] == "s1", "quantity"].iloc[0]
    assert s1_qty == 5.0  # earliest date wins


def test_derive_inventory_initial_empty_returns_schema() -> None:
    inv = derive_inventory_initial(pd.DataFrame())
    assert list(inv.columns) == ["facility_id", "commodity_category", "quantity"]
