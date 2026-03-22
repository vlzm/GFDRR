"""Attribute system: specs, grain groups, merge planning, and spine builder."""

from gbp.core.attributes.builder import AttributeBuilder
from gbp.core.attributes.defaults import (
    get_all_default_specs,
    get_edge_attribute_specs,
    get_facility_attribute_specs,
    get_resource_attribute_specs,
)
from gbp.core.attributes.grain_groups import GrainGroup, auto_group_attributes
from gbp.core.attributes.merge_plan import MergePlan, plan_merges
from gbp.core.attributes.spec import AttributeSpec

__all__ = [
    "AttributeBuilder",
    "AttributeSpec",
    "GrainGroup",
    "MergePlan",
    "auto_group_attributes",
    "get_all_default_specs",
    "get_edge_attribute_specs",
    "get_facility_attribute_specs",
    "get_resource_attribute_specs",
    "plan_merges",
]
