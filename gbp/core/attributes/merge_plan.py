"""Plan merge order for left-join spine assembly."""

from __future__ import annotations

from dataclasses import dataclass

from gbp.core.attributes.spec import AttributeSpec


@dataclass(frozen=True)
class MergePlan:
    """Represent one left-merge step when building a spine.

    Parameters
    ----------
    attribute_name
        Name of the attribute being merged.
    merge_keys
        Column names used as join keys.
    causes_expansion
        Whether the merge introduces new grain dimensions.
    expansion_dims
        New dimension columns added by this merge (empty when
        ``causes_expansion`` is ``False``).
    """

    attribute_name: str
    merge_keys: list[str]
    causes_expansion: bool
    expansion_dims: list[str]


def plan_merges(entity_grain: list[str], attributes: list[AttributeSpec]) -> list[MergePlan]:
    """Compute merge order for spine assembly.

    Free merges (no new dimensions) run first, then merges that introduce
    the fewest new dimensions. Uses each attribute's ``resolved_grain``
    (document section 10.4).

    Parameters
    ----------
    entity_grain
        Identity columns for the entity base table.
    attributes
        Attribute specs to plan merges for.

    Returns
    -------
    list of MergePlan
        Ordered merge steps, free merges before expanding ones.
    """
    if not attributes:
        return []

    current_grain = set(entity_grain)
    remaining = list(attributes)
    plans: list[MergePlan] = []

    while remaining:
        free = sorted(
            (a for a in remaining if set(a.resolved_merge_grain()).issubset(current_grain)),
            key=lambda a: (len(a.resolved_merge_grain()), a.name),
        )
        if free:
            for attr in free:
                plans.append(
                    MergePlan(
                        attribute_name=attr.name,
                        merge_keys=list(attr.resolved_merge_grain()),
                        causes_expansion=False,
                        expansion_dims=[],
                    )
                )
                remaining.remove(attr)
            continue

        remaining.sort(
            key=lambda a: (len(set(a.resolved_merge_grain()) - current_grain), a.name)
        )
        best = remaining[0]
        best_dims = set(best.resolved_merge_grain())
        new_dims = sorted(best_dims - current_grain)
        merge_keys = sorted(current_grain & best_dims, key=lambda c: (c not in entity_grain, c))
        plans.append(
            MergePlan(
                attribute_name=best.name,
                merge_keys=merge_keys,
                causes_expansion=True,
                expansion_dims=new_dims,
            )
        )
        current_grain |= best_dims
        remaining.remove(best)

    return plans
