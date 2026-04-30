"""Tests for the inventory delta helper module."""
# ruff: noqa: D102

from __future__ import annotations

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from gbp.consumers.simulator.inventory import (
    apply_delta,
    merge_with_inventory,
    to_inventory_delta,
)


def _inventory(rows: list[tuple[str, str, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        rows, columns=["facility_id", "commodity_category", "quantity"],
    )


class TestToInventoryDelta:
    """Aggregation by facility key."""

    def test_outflow_groups_by_source(self) -> None:
        flows = pd.DataFrame({
            "source_id": ["s1", "s1", "s2"],
            "target_id": ["s2", "s3", "s1"],
            "commodity_category": ["bike", "bike", "bike"],
            "quantity": [2.0, 3.0, 1.0],
        })
        delta = to_inventory_delta(flows, facility_col="source_id")
        assert list(delta.columns) == [
            "facility_id", "commodity_category", "quantity",
        ]
        # s1 outflow = 2 + 3 = 5; s2 outflow = 1
        s1 = delta[delta["facility_id"] == "s1"].iloc[0]["quantity"]
        s2 = delta[delta["facility_id"] == "s2"].iloc[0]["quantity"]
        assert s1 == 5.0
        assert s2 == 1.0

    def test_alternate_quantity_column(self) -> None:
        flows = pd.DataFrame({
            "source_id": ["s1"],
            "commodity_category": ["bike"],
            "amount": [4.0],
        })
        delta = to_inventory_delta(
            flows, facility_col="source_id", quantity_col="amount",
        )
        assert delta.iloc[0]["quantity"] == 4.0


class TestApplyDeltaSubtract:
    """Subtracting a delta moves inventory down."""

    def test_subtract_lowers_matching_rows(self) -> None:
        inv = _inventory([("s1", "bike", 10.0), ("s2", "bike", 5.0)])
        delta = _inventory([("s1", "bike", 3.0)])
        new_inv = apply_delta(inv, delta, op="subtract")
        s1 = new_inv[new_inv["facility_id"] == "s1"].iloc[0]["quantity"]
        s2 = new_inv[new_inv["facility_id"] == "s2"].iloc[0]["quantity"]
        assert s1 == 7.0
        assert s2 == 5.0  # untouched

    def test_subtract_can_go_negative(self) -> None:
        """Default subtract has no clip — caller may rely on transient negatives."""
        inv = _inventory([("s1", "bike", 2.0)])
        delta = _inventory([("s1", "bike", 5.0)])
        new_inv = apply_delta(inv, delta, op="subtract")
        assert new_inv.iloc[0]["quantity"] == -3.0


class TestApplyDeltaAdd:
    """Adding a delta moves inventory up."""

    def test_add_raises_matching_rows(self) -> None:
        inv = _inventory([("s1", "bike", 2.0)])
        delta = _inventory([("s1", "bike", 4.0)])
        new_inv = apply_delta(inv, delta, op="add")
        assert new_inv.iloc[0]["quantity"] == 6.0


class TestApplyDeltaAddClipZero:
    """add_clip_zero pulls negative results back to 0."""

    def test_clip_when_pre_existing_negative_plus_inflow_still_negative(self) -> None:
        inv = _inventory([("s1", "bike", -5.0)])
        delta = _inventory([("s1", "bike", 2.0)])
        new_inv = apply_delta(inv, delta, op="add_clip_zero")
        # -5 + 2 = -3 → clipped to 0
        assert new_inv.iloc[0]["quantity"] == 0.0

    def test_no_clip_when_result_positive(self) -> None:
        inv = _inventory([("s1", "bike", 1.0)])
        delta = _inventory([("s1", "bike", 2.0)])
        new_inv = apply_delta(inv, delta, op="add_clip_zero")
        assert new_inv.iloc[0]["quantity"] == 3.0


class TestApplyDeltaMissingRows:
    """Facilities absent from delta are treated as zero-delta."""

    def test_facility_absent_stays_unchanged(self) -> None:
        inv = _inventory([("s1", "bike", 8.0), ("s2", "bike", 3.0)])
        delta = _inventory([("s1", "bike", 1.0)])
        new_inv = apply_delta(inv, delta, op="subtract")
        s2 = new_inv[new_inv["facility_id"] == "s2"].iloc[0]["quantity"]
        assert s2 == 3.0

    def test_unknown_op_raises(self) -> None:
        inv = _inventory([("s1", "bike", 1.0)])
        delta = _inventory([("s1", "bike", 1.0)])
        with pytest.raises(ValueError, match="Unknown inventory delta op"):
            apply_delta(inv, delta, op="multiply")  # type: ignore[arg-type]

    def test_output_shape_is_inventory_columns(self) -> None:
        inv = _inventory([("s1", "bike", 5.0)])
        delta = _inventory([("s1", "bike", 1.0)])
        new_inv = apply_delta(inv, delta, op="subtract")
        assert list(new_inv.columns) == [
            "facility_id", "commodity_category", "quantity",
        ]


class TestMergeWithInventory:
    """Bare merge + fillna for bespoke arithmetic."""

    def test_value_col_is_zero_filled_for_missing(self) -> None:
        inv = _inventory([("s1", "bike", 8.0), ("s2", "bike", 3.0)])
        delta = pd.DataFrame({
            "facility_id": ["s1"],
            "commodity_category": ["bike"],
            "demand_qty": [2.0],
        })
        merged = merge_with_inventory(inv, delta, value_col="demand_qty")
        s1 = merged[merged["facility_id"] == "s1"].iloc[0]
        s2 = merged[merged["facility_id"] == "s2"].iloc[0]
        assert s1["demand_qty"] == 2.0
        assert s2["demand_qty"] == 0.0  # filled
        assert s1["quantity"] == 8.0  # inventory column preserved

    def test_caller_owns_the_arithmetic(self) -> None:
        """Helper does the boring work; arithmetic stays at the call site."""
        inv = _inventory([("s1", "bike", 8.0)])
        delta = pd.DataFrame({
            "facility_id": ["s1"],
            "commodity_category": ["bike"],
            "demand_qty": [3.0],
        })
        merged = merge_with_inventory(inv, delta, value_col="demand_qty")
        # Caller-owned arithmetic.
        merged["fulfilled"] = merged[["quantity", "demand_qty"]].min(axis=1)
        merged["deficit"] = merged["demand_qty"] - merged["fulfilled"]
        assert merged.iloc[0]["fulfilled"] == 3.0
        assert merged.iloc[0]["deficit"] == 0.0

    def test_custom_default(self) -> None:
        inv = _inventory([("s1", "bike", 8.0), ("s2", "bike", 3.0)])
        delta = pd.DataFrame({
            "facility_id": ["s1"],
            "commodity_category": ["bike"],
            "capacity": [10.0],
        })
        merged = merge_with_inventory(
            inv, delta, value_col="capacity", default=999.0,
        )
        s2 = merged[merged["facility_id"] == "s2"].iloc[0]
        assert s2["capacity"] == 999.0


class TestRoundTrip:
    """to_inventory_delta + apply_delta compose to the obvious answer."""

    def test_subtract_outflow_then_check(self) -> None:
        inv = _inventory([("s1", "bike", 10.0), ("s2", "bike", 5.0)])
        flows = pd.DataFrame({
            "source_id": ["s1", "s1"],
            "target_id": ["s2", "s2"],
            "commodity_category": ["bike", "bike"],
            "quantity": [2.0, 3.0],
        })
        delta = to_inventory_delta(flows, facility_col="source_id")
        new_inv = apply_delta(inv, delta, op="subtract")
        expected = _inventory([("s1", "bike", 5.0), ("s2", "bike", 5.0)])
        assert_frame_equal(
            new_inv.sort_values("facility_id").reset_index(drop=True),
            expected.sort_values("facility_id").reset_index(drop=True),
        )
