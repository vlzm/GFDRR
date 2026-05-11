"""Attribute system: specs, grain groups, merge planning, spine builder, and registry.

Modules
-------
spec
    ``AttributeSpec`` dataclass describing one attribute column.
grain_groups
    Cluster attributes by grain compatibility.
merge_plan
    Compute left-join merge order for spine assembly.
builder
    ``AttributeBuilder`` assembles spine tables from base entities.
registry
    ``AttributeRegistry`` stores parametric attributes with data.
defaults
    Default ``AttributeSpec`` catalog for bike-sharing scenarios.
"""

from gbp.core.attributes.builder import AttributeBuilder
from gbp.core.attributes.defaults import (
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
    "get_structural_attribute_specs",
    "plan_merges",
    "register_bike_sharing_defaults",
]
