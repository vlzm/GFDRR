"""Merge order planning for left-join spine assembly."""

from __future__ import annotations

from dataclasses import dataclass

from gbp.core.attributes.spec import AttributeSpec


@dataclass(frozen=True)
class MergePlan:
    """One left-merge step when building a spine."""

    attribute_name: str
    merge_keys: list[str]
    causes_expansion: bool
    expansion_dims: list[str]


def plan_merges(entity_grain: list[str], attributes: list[AttributeSpec]) -> list[MergePlan]:
    """Compute merge order: free (no new dimensions) merges first, then minimal expansion.

    Document §10.4. Uses each attribute's ``resolved_grain``.
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
