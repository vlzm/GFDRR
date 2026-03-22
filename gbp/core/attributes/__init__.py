"""Attribute system: specs, grain groups, merge planning, spine builder, and registry."""

from gbp.core.attributes.builder import AttributeBuilder
from gbp.core.attributes.defaults import (
    get_all_default_specs,
    get_edge_attribute_specs,
    get_facility_attribute_specs,
    get_resource_attribute_specs,
    get_structural_attribute_specs,
    register_bike_sharing_defaults,
)
from gbp.core.attributes.grain_groups import GrainGroup, auto_group_attributes
from gbp.core.attributes.merge_plan import MergePlan, plan_merges
from gbp.core.attributes.registry import AttributeRegistry, RegisteredAttribute
from gbp.core.attributes.spec import AttributeSpec

__all__ = [
    "AttributeBuilder",
    "AttributeRegistry",
    "AttributeSpec",
    "GrainGroup",
    "MergePlan",
    "RegisteredAttribute",
    "auto_group_attributes",
    "get_all_default_specs",
    "get_edge_attribute_specs",
    "get_facility_attribute_specs",
    "get_resource_attribute_specs",
    "get_structural_attribute_specs",
    "plan_merges",
    "register_bike_sharing_defaults",
]
