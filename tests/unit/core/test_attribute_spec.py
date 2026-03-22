"""Tests for ``AttributeSpec``."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from gbp.core.attributes.spec import AttributeSpec
from gbp.core.enums import AttributeKind


def test_valid_spec() -> None:
    """Construction succeeds with valid grains."""
    s = AttributeSpec(
        name="op_cost",
        kind=AttributeKind.COST,
        entity_type="facility",
        grain=("facility_id", "date"),
        resolved_grain=("facility_id", "period_id"),
        value_column="cost_per_unit",
        source_table="operation_costs",
    )
    assert s.time_varying is True
    assert s.entity_grain == ("facility_id",)


def test_entity_grain_edge_and_resource() -> None:
    """Edge and resource entity grains match model FK columns."""
    e = AttributeSpec(
        name="tc",
        kind=AttributeKind.COST,
        entity_type="edge",
        grain=("source_id", "target_id", "modal_type", "date"),
        resolved_grain=("source_id", "target_id", "modal_type", "period_id"),
        value_column="cost_per_unit",
        source_table="transport_costs",
    )
    assert e.entity_grain == ("source_id", "target_id", "modal_type")

    r = AttributeSpec(
        name="rc",
        kind=AttributeKind.COST,
        entity_type="resource",
        grain=("resource_category", "date"),
        resolved_grain=("resource_category", "period_id"),
        value_column="value",
        source_table="resource_costs",
    )
    assert r.entity_grain == ("resource_category",)


def test_invalid_entity_type() -> None:
    """Unknown entity_type raises."""
    with pytest.raises(ValueError, match="entity_type"):
        AttributeSpec(
            name="x",
            kind=AttributeKind.ADDITIONAL,
            entity_type="planet",
            grain=("facility_id",),
            resolved_grain=("facility_id",),
            value_column="v",
            source_table="t",
        )


def test_entity_grain_not_subset_of_grain() -> None:
    """Entity keys must appear in raw grain."""
    with pytest.raises(ValueError, match="entity_grain"):
        AttributeSpec(
            name="x",
            kind=AttributeKind.ADDITIONAL,
            entity_type="facility",
            grain=("wrong_id", "date"),
            resolved_grain=("wrong_id", "period_id"),
            value_column="v",
            source_table="t",
        )


def test_time_varying_requires_period_in_resolved() -> None:
    """Resolved grain must use period_id when raw uses date."""
    with pytest.raises(ValueError, match="period_id"):
        AttributeSpec(
            name="x",
            kind=AttributeKind.COST,
            entity_type="facility",
            grain=("facility_id", "date"),
            resolved_grain=("facility_id", "wrong_period"),
            value_column="v",
            source_table="t",
        )


def test_resolved_grain_must_not_contain_date() -> None:
    """Resolved grain must not keep calendar date."""
    with pytest.raises(ValueError, match="resolved_grain"):
        AttributeSpec(
            name="x",
            kind=AttributeKind.COST,
            entity_type="facility",
            grain=("facility_id", "date"),
            resolved_grain=("facility_id", "date"),
            value_column="v",
            source_table="t",
        )


def test_frozen_immutability() -> None:
    """AttributeSpec is frozen."""
    s = AttributeSpec(
        name="a",
        kind=AttributeKind.ADDITIONAL,
        entity_type="facility",
        grain=("facility_id",),
        resolved_grain=("facility_id",),
        value_column="v",
        source_table="t",
    )
    with pytest.raises(FrozenInstanceError):
        s.name = "b"  # type: ignore[misc]
