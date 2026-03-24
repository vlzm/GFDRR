"""Tests for ``AttributeBuilder``."""

from __future__ import annotations

import pandas as pd
import pytest

from gbp.core.attributes.builder import AttributeBuilder
from gbp.core.attributes.spec import AttributeSpec
from gbp.core.enums import AttributeKind


def _fac_spec(
    name: str,
    resolved: tuple[str, ...],
    *,
    nullable: bool = True,
    kind: AttributeKind = AttributeKind.COST,
) -> AttributeSpec:
    grain = tuple("date" if x == "period_id" else x for x in resolved)
    return AttributeSpec(
        name=name,
        kind=kind,
        entity_type="facility",
        grain=grain,
        resolved_grain=resolved,
        value_column="v",
        source_table="t",
        nullable=nullable,
    )


def test_register_wrong_entity_type() -> None:
    """Spec entity_type must match builder."""
    b = AttributeBuilder("facility")
    s = AttributeSpec(
        name="e",
        kind=AttributeKind.ADDITIONAL,
        entity_type="edge",
        grain=("source_id", "target_id", "modal_type"),
        resolved_grain=("source_id", "target_id", "modal_type"),
        value_column="d",
        source_table="edges",
    )
    with pytest.raises(ValueError, match="entity_type"):
        b.register(s)


def test_build_single_group_single_attribute() -> None:
    """One nullable attribute merged onto facility base."""
    s = _fac_spec("cost", ("facility_id", "period_id"))
    base = pd.DataFrame({"facility_id": ["a", "b"]})
    data = pd.DataFrame(
        {
            "facility_id": ["a", "b"],
            "period_id": ["p0", "p0"],
            "v": [1.0, 2.0],
        }
    )
    b = AttributeBuilder("facility")
    b.register(s)
    out = b.build_spines(base, {"cost": data})
    assert len(out) == 1
    spine = next(iter(out.values()))
    assert "cost" in spine.columns
    assert spine.loc[spine["facility_id"] == "a", "cost"].iloc[0] == 1.0


def test_missing_required_attribute_raises() -> None:
    """Non-nullable attribute without data raises."""
    s = _fac_spec("cost", ("facility_id", "period_id"), nullable=False)
    b = AttributeBuilder("facility")
    b.register(s)
    with pytest.raises(ValueError, match="Missing required"):
        b.build_spines(pd.DataFrame({"facility_id": ["a"]}), {})


def test_nullable_missing_skipped() -> None:
    """Nullable attribute with no data is skipped (no merge, no new column)."""
    s = _fac_spec("extra", ("facility_id", "period_id"), nullable=True)
    b = AttributeBuilder("facility")
    b.register(s)
    out = b.build_spines(pd.DataFrame({"facility_id": ["a"]}), {})
    spine = next(iter(out.values()))
    assert "extra" not in spine.columns
    assert list(spine.columns) == ["facility_id"]


def test_negative_cost_raises() -> None:
    """COST kind rejects negative values."""
    s = _fac_spec("cost", ("facility_id", "period_id"), nullable=False)
    b = AttributeBuilder("facility")
    b.register(s)
    data = pd.DataFrame({"facility_id": ["a"], "period_id": ["p0"], "v": [-1.0]})
    with pytest.raises(ValueError, match="negative"):
        b.build_spines(pd.DataFrame({"facility_id": ["a"]}), {"cost": data})


def test_zero_capacity_raises() -> None:
    """CAPACITY kind rejects non-positive values."""
    s = AttributeSpec(
        name="cap",
        kind=AttributeKind.CAPACITY,
        entity_type="facility",
        grain=("facility_id",),
        resolved_grain=("facility_id",),
        value_column="capacity",
        source_table="operation_capacities",
        nullable=False,
    )
    b = AttributeBuilder("facility")
    b.register(s)
    data = pd.DataFrame({"facility_id": ["a"], "capacity": [0.0]})
    with pytest.raises(ValueError, match="non-positive"):
        b.build_spines(pd.DataFrame({"facility_id": ["a"]}), {"cap": data})


def test_all_non_numeric_values_raises() -> None:
    """Value column with all non-numeric strings is rejected."""
    s = _fac_spec("bad", ("facility_id",))
    b = AttributeBuilder("facility")
    b.register(s)
    data = pd.DataFrame({"facility_id": ["a", "b"], "v": ["foo", "bar"]})
    with pytest.raises(ValueError, match="no numeric values"):
        b.build_spines(pd.DataFrame({"facility_id": ["a", "b"]}), {"bad": data})


def test_no_shared_join_keys_error_is_actionable() -> None:
    """Error message includes resolved_grain, and hints about common causes."""
    from unittest.mock import patch

    s = _fac_spec("cost", ("facility_id", "period_id"))
    b = AttributeBuilder("facility")
    b.register(s)
    data = pd.DataFrame({"facility_id": ["a"], "period_id": ["p0"], "v": [1.0]})
    base = pd.DataFrame({"facility_id": ["a"]})

    # Patch _prepare_attribute_frame to return a frame with no overlapping columns
    no_overlap = pd.DataFrame({"alien_col": [1.0], "cost": [1.0]})
    with patch(
        "gbp.core.attributes.builder._prepare_attribute_frame", return_value=no_overlap
    ):
        with pytest.raises(ValueError, match="resolved_grain") as exc_info:
            b.build_spines(base, {"cost": data})
    msg = str(exc_info.value)
    assert "time resolution" in msg


def test_eav_filter() -> None:
    """EAV filter selects rows before merge."""
    s = AttributeSpec(
        name="fixed",
        kind=AttributeKind.COST,
        entity_type="resource",
        grain=("resource_category", "date"),
        resolved_grain=("resource_category", "period_id"),
        value_column="value",
        source_table="resource_costs",
        nullable=False,
        eav_filter={"attribute_name": "fixed_cost"},
    )
    b = AttributeBuilder("resource")
    b.register(s)
    base = pd.DataFrame({"resource_category": ["t1"]})
    raw = pd.DataFrame(
        {
            "resource_category": ["t1", "t1"],
            "period_id": ["p0", "p0"],
            "attribute_name": ["fixed_cost", "other"],
            "value": [10.0, 99.0],
        }
    )
    out = b.build_spines(base, {"fixed": raw})
    spine = next(iter(out.values()))
    assert spine["fixed"].iloc[0] == 10.0
