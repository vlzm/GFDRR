"""Grain groups: cluster attributes to avoid cross-join spine explosions."""

from __future__ import annotations

from dataclasses import dataclass, field

from gbp.core.attributes.spec import AttributeSpec


@dataclass
class GrainGroup:
    """A set of attributes whose grains form a chain (subset/superset) on dimensions."""

    name: str
    grain: list[str]
    attributes: list[AttributeSpec] = field(default_factory=list)


def auto_group_attributes(
    entity_grain: list[str],
    attributes: list[AttributeSpec],
) -> list[GrainGroup]:
    """Place each attribute in a group where grains are chain-compatible.

    Two grains are compatible if one dimension set is a subset of the other.
    When an attribute fits an existing group, the group's grain becomes the union
    of dimensions (document §10.3).
    """
    if not attributes:
        return []

    entity_dims = set(entity_grain)
    sorted_attrs = sorted(attributes, key=lambda a: len(a.resolved_merge_grain()))
    groups: list[GrainGroup] = []

    for attr in sorted_attrs:
        attr_dims = set(attr.resolved_merge_grain())
        if not entity_dims.issubset(attr_dims):
            raise ValueError(
                f"Attribute {attr.name!r} resolved grain {attr.resolved_grain} "
                f"must contain entity grain {entity_grain}"
            )

        placed = False
        for group in groups:
            group_dims = set(group.grain)
            if attr_dims.issubset(group_dims) or group_dims.issubset(attr_dims):
                group.attributes.append(attr)
                group.grain = sorted(
                    group_dims | attr_dims,
                    key=lambda c: (c not in entity_grain, c),
                )
                placed = True
                break

        if not placed:
            groups.append(
                GrainGroup(
                    name=f"group_{len(groups)}",
                    grain=sorted(attr_dims, key=lambda c: (c not in entity_grain, c)),
                    attributes=[attr],
                )
            )

    return groups
