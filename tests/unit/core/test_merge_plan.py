"""Tests for merge planning."""

from __future__ import annotations

from gbp.core.attributes.merge_plan import plan_merges
from gbp.core.attributes.spec import AttributeSpec
from gbp.core.enums import AttributeKind


def _f(name: str, resolved: tuple[str, ...]) -> AttributeSpec:
    grain = tuple("date" if x == "period_id" else x for x in resolved)
    return AttributeSpec(
        name=name,
        kind=AttributeKind.COST,
        entity_type="facility",
        grain=grain,
        resolved_grain=resolved,
        value_column="v",
        source_table="t",
        nullable=True,
    )


def test_empty() -> None:
    """No attributes yields no merge steps."""
    assert plan_merges(["facility_id"], []) == []


def test_free_merges_only() -> None:
    """Attributes at entity grain only: all merge steps are free."""
    a = _f("a", ("facility_id",))
    b = _f("b", ("facility_id",))
    plans = plan_merges(["facility_id"], [b, a])
    assert all(not p.causes_expansion for p in plans)
    assert {p.attribute_name for p in plans} == {"a", "b"}


def test_single_expansion() -> None:
    """One attribute extends grain beyond entity keys."""
    wide = _f("wide", ("facility_id", "commodity_category", "period_id"))
    plans = plan_merges(["facility_id"], [wide])
    assert len(plans) == 1
    assert plans[0].causes_expansion is True
    assert set(plans[0].expansion_dims) == {"commodity_category", "period_id"}


def test_free_before_expansion() -> None:
    """Free merges run before any expansion step."""
    base = _f("base", ("facility_id",))
    wide = _f("wide", ("facility_id", "operation_type", "period_id"))
    plans = plan_merges(["facility_id"], [wide, base])
    assert plans[0].causes_expansion is False
    assert plans[0].attribute_name == "base"
    assert any(p.causes_expansion for p in plans[1:])


def test_smallest_expansion_first() -> None:
    """Among expansions, fewer new dimensions wins."""
    big = _f("big", ("facility_id", "a", "b", "period_id"))
    small = _f("small", ("facility_id", "period_id"))
    plans = plan_merges(["facility_id"], [big, small])
    expand_steps = [p for p in plans if p.causes_expansion]
    assert expand_steps[0].attribute_name == "small"
