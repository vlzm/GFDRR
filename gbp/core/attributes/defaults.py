"""Default ``AttributeSpec`` catalog for bike-sharing (L3) scenarios.

Provides two categories of attribute specs:

- **Structural**: derive values from structural tables (``edges``,
  ``resource_categories``).  Their ``source_table`` points to a fixed
  ``RawModelData`` field.
- **Parametric**: previously stored in dedicated fixed fields
  (``operation_costs``, ``transport_costs``, …) now registered via
  ``AttributeRegistry``.

``register_bike_sharing_defaults()`` is the convenience entry-point for
the bike-sharing domain that registers typical parametric attributes
with standard grains.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from gbp.core.attributes.spec import AttributeSpec
from gbp.core.enums import AttributeKind

if TYPE_CHECKING:
    from gbp.core.attributes.registry import AttributeRegistry


# ── Structural attribute specs (data lives in structural tables) ─────────

def get_structural_attribute_specs() -> list[AttributeSpec]:
    """Attributes sourced from structural tables (edges, resource_categories).

    These specs are NOT registered in the ``AttributeRegistry``; their data
    is loaded from the corresponding ``source_table`` field on
    ``ResolvedModelData`` during spine assembly.
    """
    return [
        AttributeSpec(
            name="edge_distance",
            kind=AttributeKind.ADDITIONAL,
            entity_type="edge",
            grain=("source_id", "target_id", "modal_type"),
            resolved_grain=("source_id", "target_id", "modal_type"),
            value_column="distance",
            source_table="edges",
            unit=None,
            aggregation="mean",
            nullable=False,
        ),
        AttributeSpec(
            name="edge_lead_time_hours",
            kind=AttributeKind.ADDITIONAL,
            entity_type="edge",
            grain=("source_id", "target_id", "modal_type"),
            resolved_grain=("source_id", "target_id", "modal_type"),
            value_column="lead_time_hours",
            source_table="edges",
            unit=None,
            aggregation="mean",
            nullable=False,
        ),
        AttributeSpec(
            name="edge_reliability",
            kind=AttributeKind.ADDITIONAL,
            entity_type="edge",
            grain=("source_id", "target_id", "modal_type"),
            resolved_grain=("source_id", "target_id", "modal_type"),
            value_column="reliability",
            source_table="edges",
            unit=None,
            aggregation="mean",
            nullable=True,
        ),
        AttributeSpec(
            name="resource_base_capacity",
            kind=AttributeKind.CAPACITY,
            entity_type="resource",
            grain=("resource_category",),
            resolved_grain=("resource_category",),
            value_column="base_capacity",
            source_table="resource_categories",
            unit=None,
            aggregation="min",
            nullable=False,
        ),
    ]


# ── Legacy parametric specs (kept for backward compatibility) ────────────

def get_facility_attribute_specs() -> list[AttributeSpec]:
    """Facility-level attributes mapped to core model tables."""
    return [
        AttributeSpec(
            name="operation_cost",
            kind=AttributeKind.COST,
            entity_type="facility",
            grain=(
                "facility_id",
                "operation_type",
                "commodity_category",
                "date",
            ),
            resolved_grain=(
                "facility_id",
                "operation_type",
                "commodity_category",
                "period_id",
            ),
            value_column="cost_per_unit",
            source_table="operation_costs",
            unit=None,
            aggregation="mean",
            nullable=True,
        ),
        AttributeSpec(
            name="operation_capacity",
            kind=AttributeKind.CAPACITY,
            entity_type="facility",
            grain=("facility_id", "operation_type", "commodity_category"),
            resolved_grain=("facility_id", "operation_type", "commodity_category"),
            value_column="capacity",
            source_table="operation_capacities",
            unit=None,
            aggregation="min",
            nullable=True,
        ),
    ]


def get_edge_attribute_specs() -> list[AttributeSpec]:
    """Edge-level attributes (structural + parametric)."""
    structural = [s for s in get_structural_attribute_specs() if s.entity_type == "edge"]
    parametric = [
        AttributeSpec(
            name="transport_cost",
            kind=AttributeKind.COST,
            entity_type="edge",
            grain=("source_id", "target_id", "modal_type", "resource_category", "date"),
            resolved_grain=(
                "source_id",
                "target_id",
                "modal_type",
                "resource_category",
                "period_id",
            ),
            value_column="cost_per_unit",
            source_table="transport_costs",
            unit=None,
            aggregation="mean",
            nullable=True,
        ),
        AttributeSpec(
            name="edge_capacity",
            kind=AttributeKind.CAPACITY,
            entity_type="edge",
            grain=("source_id", "target_id", "modal_type", "date"),
            resolved_grain=("source_id", "target_id", "modal_type", "period_id"),
            value_column="capacity",
            source_table="edge_capacities",
            unit=None,
            aggregation="min",
            nullable=True,
        ),
    ]
    return structural + parametric


def get_resource_attribute_specs() -> list[AttributeSpec]:
    """Resource-category attributes (structural + EAV ``resource_costs``)."""
    structural = [s for s in get_structural_attribute_specs() if s.entity_type == "resource"]
    parametric = [
        AttributeSpec(
            name="resource_fixed_cost",
            kind=AttributeKind.COST,
            entity_type="resource",
            grain=("resource_category", "date"),
            resolved_grain=("resource_category", "period_id"),
            value_column="value",
            source_table="resource_costs",
            unit=None,
            aggregation="mean",
            nullable=True,
            eav_filter={"attribute_name": "fixed_cost_per_period"},
        ),
        AttributeSpec(
            name="resource_maintenance_cost",
            kind=AttributeKind.COST,
            entity_type="resource",
            grain=("resource_category", "date"),
            resolved_grain=("resource_category", "period_id"),
            value_column="value",
            source_table="resource_costs",
            unit=None,
            aggregation="mean",
            nullable=True,
            eav_filter={"attribute_name": "maintenance_cost"},
        ),
    ]
    return structural + parametric


def get_all_default_specs() -> list[AttributeSpec]:
    """All default bike-sharing attribute specs (structural + parametric)."""
    return (
        get_facility_attribute_specs()
        + get_edge_attribute_specs()
        + get_resource_attribute_specs()
    )


# ── Convenience registration for bike-sharing ────────────────────────────

def register_bike_sharing_defaults(
    registry: AttributeRegistry,
    *,
    operation_costs: pd.DataFrame | None = None,
    transport_costs: pd.DataFrame | None = None,
    operation_capacities: pd.DataFrame | None = None,
    edge_capacities: pd.DataFrame | None = None,
    resource_costs: pd.DataFrame | None = None,
    commodity_sell_price_tiers: pd.DataFrame | None = None,
    commodity_procurement_cost_tiers: pd.DataFrame | None = None,
) -> None:
    """Register standard bike-sharing parametric attributes.

    Convenience wrapper that registers known attributes with typical grains.
    Users can also call ``registry.register()`` directly for custom
    attributes or non-standard grains.
    """
    if operation_costs is not None:
        registry.register(
            name="operation_cost",
            data=operation_costs,
            entity_type="facility",
            kind=AttributeKind.COST,
            grain=("facility_id", "operation_type", "commodity_category", "date"),
            value_column="cost_per_unit",
            aggregation="mean",
        )
    if operation_capacities is not None:
        registry.register(
            name="operation_capacity",
            data=operation_capacities,
            entity_type="facility",
            kind=AttributeKind.CAPACITY,
            grain=("facility_id", "operation_type", "commodity_category"),
            value_column="capacity",
            aggregation="min",
        )
    if transport_costs is not None:
        registry.register(
            name="transport_cost",
            data=transport_costs,
            entity_type="edge",
            kind=AttributeKind.COST,
            grain=("source_id", "target_id", "modal_type", "resource_category", "date"),
            value_column="cost_per_unit",
            aggregation="mean",
        )
    if edge_capacities is not None:
        registry.register(
            name="edge_capacity",
            data=edge_capacities,
            entity_type="edge",
            kind=AttributeKind.CAPACITY,
            grain=("source_id", "target_id", "modal_type", "date"),
            value_column="capacity",
            aggregation="min",
        )
    if resource_costs is not None:
        registry.register(
            name="resource_fixed_cost",
            data=resource_costs,
            entity_type="resource",
            kind=AttributeKind.COST,
            grain=("resource_category", "date"),
            value_column="value",
            aggregation="mean",
            eav_filter={"attribute_name": "fixed_cost_per_period"},
        )
        registry.register(
            name="resource_maintenance_cost",
            data=resource_costs,
            entity_type="resource",
            kind=AttributeKind.COST,
            grain=("resource_category", "date"),
            value_column="value",
            aggregation="mean",
            eav_filter={"attribute_name": "maintenance_cost"},
        )
    if commodity_sell_price_tiers is not None:
        registry.register(
            name="sell_price",
            data=commodity_sell_price_tiers,
            entity_type="facility",
            kind=AttributeKind.REVENUE,
            grain=(
                "facility_id", "commodity_category",
                "tier_index", "min_volume", "max_volume", "date",
            ),
            value_column="price_per_unit",
            aggregation="mean",
        )
    if commodity_procurement_cost_tiers is not None:
        registry.register(
            name="procurement_cost",
            data=commodity_procurement_cost_tiers,
            entity_type="facility",
            kind=AttributeKind.COST,
            grain=(
                "facility_id", "commodity_category",
                "tier_index", "min_volume", "max_volume", "date",
            ),
            value_column="cost_per_unit",
            aggregation="mean",
        )
