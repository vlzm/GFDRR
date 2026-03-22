"""Tests for grain group auto-clustering."""

from __future__ import annotations

from gbp.core.attributes.grain_groups import GrainGroup, auto_group_attributes
from gbp.core.attributes.spec import AttributeSpec
from gbp.core.enums import AttributeKind


def _spec(
    name: str,
    entity_type: str,
    resolved: tuple[str, ...],
    *,
    grain: tuple[str, ...] | None = None,
) -> AttributeSpec:
    g = grain if grain is not None else tuple("date" if x == "period_id" else x for x in resolved)
    g = tuple("date" if x == "period_id" else x for x in g)
    return AttributeSpec(
        name=name,
        kind=AttributeKind.COST,
        entity_type=entity_type,
        grain=g,
        resolved_grain=resolved,
        value_column="v",
        source_table="t",
        nullable=True,
    )


def test_empty_attributes() -> None:
    """No attributes yields no groups."""
    assert auto_group_attributes(["facility_id"], []) == []


def test_single_attribute_one_group() -> None:
    """One attribute produces one group."""
    a = _spec("a", "facility", ("facility_id", "period_id"))
    groups = auto_group_attributes(["facility_id"], [a])
    assert len(groups) == 1
    assert groups[0].attributes == [a]


def test_two_chain_compatible_same_group() -> None:
    """Subset/superset grains stay in one group."""
    small = _spec("small", "facility", ("facility_id",))
    large = _spec("large", "facility", ("facility_id", "commodity_category", "period_id"))
    groups = auto_group_attributes(["facility_id"], [large, small])
    assert len(groups) == 1
    assert set(groups[0].grain) == {"facility_id", "commodity_category", "period_id"}


def test_independent_dims_separate_groups() -> None:
    """Orthogonal dimensions (op vs commodity) form two groups when incompatible."""
    g1 = _spec("g1", "facility", ("facility_id", "commodity_category", "period_id"))
    g2 = _spec("g2", "facility", ("facility_id", "operation_type", "period_id"))
    groups = auto_group_attributes(["facility_id"], [g1, g2])
    assert len(groups) == 2


def test_doc_style_facility_mix_two_groups() -> None:
    """Auto-group merges facility-only attr into the wider commodity×period group."""
    type_attr = _spec("ftype", "facility", ("facility_id",))
    cap = _spec("cap", "facility", ("facility_id", "commodity_category", "period_id"))
    op_cost = _spec("op", "facility", ("facility_id", "operation_type", "period_id"))
    groups = auto_group_attributes(["facility_id"], [cap, op_cost, type_attr])
    assert len(groups) == 2
    names_sets = [{g.name for g in grp.attributes} for grp in groups]
    assert any("ftype" in ns and "cap" in ns for ns in names_sets)
    assert any("op" in ns for ns in names_sets)


def test_grain_group_dataclass() -> None:
    """GrainGroup holds name and attributes."""
    a = _spec("a", "edge", ("source_id", "target_id", "modal_type"))
    g = GrainGroup(name="g0", grain=["source_id", "target_id", "modal_type"], attributes=[a])
    assert g.name == "g0"
    assert g.attributes[0].name == "a"
