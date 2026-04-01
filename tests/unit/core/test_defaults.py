"""Tests for default bike-sharing attribute specs."""

from __future__ import annotations

from dataclasses import fields

from gbp.core.attributes.defaults import get_structural_attribute_specs
from gbp.core.model import RawModelData


def test_structural_specs_entity_types() -> None:
    """Structural specs target edge or resource entities."""
    specs = get_structural_attribute_specs()
    assert len(specs) > 0
    for s in specs:
        assert s.entity_type in ("facility", "edge", "resource")


def test_source_tables_exist_on_raw_model() -> None:
    """Every default spec references a ``RawModelData`` field."""
    raw_field_names = {f.name for f in fields(RawModelData) if not f.name.startswith("_")}
    for s in get_structural_attribute_specs():
        assert s.source_table in raw_field_names, s.source_table


def test_unique_default_names() -> None:
    """Default spec names are unique."""
    names = [s.name for s in get_structural_attribute_specs()]
    assert len(names) == len(set(names))
