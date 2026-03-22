"""Default ``AttributeSpec`` catalog for bike-sharing (L3) scenarios."""

from __future__ import annotations

from gbp.core.attributes.spec import AttributeSpec
from gbp.core.enums import AttributeKind


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
    """Edge-level attributes (static edge table + time-varying costs/capacities)."""
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


def get_resource_attribute_specs() -> list[AttributeSpec]:
    """Resource-category attributes (EAV ``resource_costs`` + static base capacity)."""
    return [
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


def get_all_default_specs() -> list[AttributeSpec]:
    """All default bike-sharing attribute specs."""
    return (
        get_facility_attribute_specs()
        + get_edge_attribute_specs()
        + get_resource_attribute_specs()
    )
