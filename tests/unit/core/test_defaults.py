"""Tests for default bike-sharing attribute specs."""

from __future__ import annotations

from dataclasses import fields

from gbp.core.attributes.defaults import (
    get_all_default_specs,
    get_edge_attribute_specs,
    get_facility_attribute_specs,
    get_resource_attribute_specs,
)
from gbp.core.model import RawModelData


def test_facility_specs_entity_type() -> None:
    """Facility defaults target facility entity."""
    for s in get_facility_attribute_specs():
        assert s.entity_type == "facility"


def test_edge_specs_entity_type() -> None:
    """Edge defaults target edge entity."""
    for s in get_edge_attribute_specs():
        assert s.entity_type == "edge"


def test_resource_specs_entity_type() -> None:
    """Resource defaults target resource entity."""
    for s in get_resource_attribute_specs():
        assert s.entity_type == "resource"


def test_all_default_specs_union() -> None:
    """Combined catalog is the concatenation of the three getters."""
    all_s = get_all_default_specs()
    assert len(all_s) == (
        len(get_facility_attribute_specs())
        + len(get_edge_attribute_specs())
        + len(get_resource_attribute_specs())
    )


def test_source_tables_exist_on_raw_model() -> None:
    """Every default spec references a ``RawModelData`` field."""
    raw_field_names = {f.name for f in fields(RawModelData) if not f.name.startswith("_")}
    for s in get_all_default_specs():
        assert s.source_table in raw_field_names, s.source_table


def test_unique_default_names() -> None:
    """Default spec names are unique."""
    names = [s.name for s in get_all_default_specs()]
    assert len(names) == len(set(names))
