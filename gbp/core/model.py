"""Container contracts for raw and resolved tabular model data."""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import ClassVar

import pandas as pd
from pydantic import BaseModel

from gbp.core.schemas import (
    Commodity,
    CommodityCategory,
    CommodityHierarchyLevel,
    CommodityHierarchyMembership,
    CommodityHierarchyNode,
    CommodityHierarchyType,
    CommodityProcurementCostTier,
    CommoditySellPriceTier,
    Demand,
    Edge,
    EdgeCapacity,
    EdgeCommodity,
    EdgeCommodityCapacity,
    EdgeLeadTimeResolved,
    EdgeRule,
    EdgeVehicle,
    Facility,
    FacilityAvailability,
    FacilityHierarchyLevel,
    FacilityHierarchyMembership,
    FacilityHierarchyNode,
    FacilityHierarchyType,
    FacilityOperation,
    FacilityRoleRecord,
    InventoryInitial,
    InventoryInTransit,
    OperationCapacity,
    OperationCost,
    Period,
    PlanningHorizon,
    PlanningHorizonSegment,
    Resource,
    ResourceAvailability,
    ResourceCategory,
    ResourceCommodityCompatibility,
    ResourceCost,
    ResourceFleet,
    ResourceModalCompatibility,
    Scenario,
    ScenarioEdgeRules,
    ScenarioManualEdges,
    ScenarioParameterOverrides,
    Supply,
    Transformation,
    TransformationInput,
    TransformationOutput,
    TransportCost,
)


def _required_column_names(row_model: type[BaseModel]) -> list[str]:
    """Return field names that must appear as DataFrame columns."""
    return [name for name, fi in row_model.model_fields.items() if fi.is_required()]


def _validate_dataframe_columns(
    name: str,
    df: pd.DataFrame,
    row_model: type[BaseModel],
) -> list[str]:
    """Return error messages for column mismatches (empty if ok)."""
    missing = [c for c in _required_column_names(row_model) if c not in df.columns]
    if missing:
        return [f"{name}: missing required columns {missing}"]
    return []


@dataclass
class RawModelData:
    """Raw input tables keyed by ``date`` where time-varying (pre-resolution)."""

    facilities: pd.DataFrame
    commodity_categories: pd.DataFrame
    resource_categories: pd.DataFrame
    planning_horizon: pd.DataFrame
    planning_horizon_segments: pd.DataFrame
    periods: pd.DataFrame
    facility_roles: pd.DataFrame
    facility_operations: pd.DataFrame
    edge_rules: pd.DataFrame

    resources: pd.DataFrame | None = None
    commodities: pd.DataFrame | None = None
    facility_availability: pd.DataFrame | None = None
    transformations: pd.DataFrame | None = None
    transformation_inputs: pd.DataFrame | None = None
    transformation_outputs: pd.DataFrame | None = None
    resource_commodity_compatibility: pd.DataFrame | None = None
    resource_modal_compatibility: pd.DataFrame | None = None
    resource_fleet: pd.DataFrame | None = None
    resource_availability: pd.DataFrame | None = None
    edges: pd.DataFrame | None = None
    edge_commodities: pd.DataFrame | None = None
    edge_capacities: pd.DataFrame | None = None
    edge_commodity_capacities: pd.DataFrame | None = None
    edge_vehicles: pd.DataFrame | None = None
    demand: pd.DataFrame | None = None
    supply: pd.DataFrame | None = None
    inventory_initial: pd.DataFrame | None = None
    inventory_in_transit: pd.DataFrame | None = None
    operation_capacities: pd.DataFrame | None = None
    operation_costs: pd.DataFrame | None = None
    transport_costs: pd.DataFrame | None = None
    resource_costs: pd.DataFrame | None = None
    commodity_sell_price_tiers: pd.DataFrame | None = None
    commodity_procurement_cost_tiers: pd.DataFrame | None = None
    facility_hierarchy_types: pd.DataFrame | None = None
    facility_hierarchy_levels: pd.DataFrame | None = None
    facility_hierarchy_nodes: pd.DataFrame | None = None
    facility_hierarchy_memberships: pd.DataFrame | None = None
    commodity_hierarchy_types: pd.DataFrame | None = None
    commodity_hierarchy_levels: pd.DataFrame | None = None
    commodity_hierarchy_nodes: pd.DataFrame | None = None
    commodity_hierarchy_memberships: pd.DataFrame | None = None
    scenarios: pd.DataFrame | None = None
    scenario_edge_rules: pd.DataFrame | None = None
    scenario_manual_edges: pd.DataFrame | None = None
    scenario_parameter_overrides: pd.DataFrame | None = None

    _SCHEMAS: ClassVar[dict[str, type[BaseModel]]] = {
        "facilities": Facility,
        "commodity_categories": CommodityCategory,
        "resource_categories": ResourceCategory,
        "planning_horizon": PlanningHorizon,
        "planning_horizon_segments": PlanningHorizonSegment,
        "periods": Period,
        "facility_roles": FacilityRoleRecord,
        "facility_operations": FacilityOperation,
        "edge_rules": EdgeRule,
        "resources": Resource,
        "commodities": Commodity,
        "facility_availability": FacilityAvailability,
        "transformations": Transformation,
        "transformation_inputs": TransformationInput,
        "transformation_outputs": TransformationOutput,
        "resource_commodity_compatibility": ResourceCommodityCompatibility,
        "resource_modal_compatibility": ResourceModalCompatibility,
        "resource_fleet": ResourceFleet,
        "resource_availability": ResourceAvailability,
        "edges": Edge,
        "edge_commodities": EdgeCommodity,
        "edge_capacities": EdgeCapacity,
        "edge_commodity_capacities": EdgeCommodityCapacity,
        "edge_vehicles": EdgeVehicle,
        "demand": Demand,
        "supply": Supply,
        "inventory_initial": InventoryInitial,
        "inventory_in_transit": InventoryInTransit,
        "operation_capacities": OperationCapacity,
        "operation_costs": OperationCost,
        "transport_costs": TransportCost,
        "resource_costs": ResourceCost,
        "commodity_sell_price_tiers": CommoditySellPriceTier,
        "commodity_procurement_cost_tiers": CommodityProcurementCostTier,
        "facility_hierarchy_types": FacilityHierarchyType,
        "facility_hierarchy_levels": FacilityHierarchyLevel,
        "facility_hierarchy_nodes": FacilityHierarchyNode,
        "facility_hierarchy_memberships": FacilityHierarchyMembership,
        "commodity_hierarchy_types": CommodityHierarchyType,
        "commodity_hierarchy_levels": CommodityHierarchyLevel,
        "commodity_hierarchy_nodes": CommodityHierarchyNode,
        "commodity_hierarchy_memberships": CommodityHierarchyMembership,
        "scenarios": Scenario,
        "scenario_edge_rules": ScenarioEdgeRules,
        "scenario_manual_edges": ScenarioManualEdges,
        "scenario_parameter_overrides": ScenarioParameterOverrides,
    }

    _REQUIRED: ClassVar[frozenset[str]] = frozenset(
        {
            "facilities",
            "commodity_categories",
            "resource_categories",
            "planning_horizon",
            "planning_horizon_segments",
            "periods",
            "facility_roles",
            "facility_operations",
            "edge_rules",
        }
    )

    def validate(self) -> None:
        """Check required tables exist and columns match row schemas."""
        errors: list[str] = []
        for f in fields(self):
            if f.name.startswith("_") or f.name not in self._SCHEMAS:
                continue
            df = getattr(self, f.name)
            if f.name in self._REQUIRED:
                if df is None:
                    errors.append(f"{f.name} is required but is None")
                    continue
                errors.extend(_validate_dataframe_columns(f.name, df, self._SCHEMAS[f.name]))
            elif df is not None:
                errors.extend(_validate_dataframe_columns(f.name, df, self._SCHEMAS[f.name]))

        if errors:
            raise ValueError("RawModelData validation failed: " + "; ".join(errors))


@dataclass
class ResolvedModelData:
    """Tables after time resolution (``period_id``) plus generated artifacts."""

    facilities: pd.DataFrame
    commodity_categories: pd.DataFrame
    resource_categories: pd.DataFrame
    planning_horizon: pd.DataFrame
    planning_horizon_segments: pd.DataFrame
    periods: pd.DataFrame
    facility_roles: pd.DataFrame
    facility_operations: pd.DataFrame
    edge_rules: pd.DataFrame

    resources: pd.DataFrame | None = None
    commodities: pd.DataFrame | None = None
    facility_availability: pd.DataFrame | None = None
    transformations: pd.DataFrame | None = None
    transformation_inputs: pd.DataFrame | None = None
    transformation_outputs: pd.DataFrame | None = None
    resource_commodity_compatibility: pd.DataFrame | None = None
    resource_modal_compatibility: pd.DataFrame | None = None
    resource_fleet: pd.DataFrame | None = None
    resource_availability: pd.DataFrame | None = None
    edges: pd.DataFrame | None = None
    edge_commodities: pd.DataFrame | None = None
    edge_capacities: pd.DataFrame | None = None
    edge_commodity_capacities: pd.DataFrame | None = None
    edge_vehicles: pd.DataFrame | None = None
    edge_lead_time_resolved: pd.DataFrame | None = None
    transformation_resolved: pd.DataFrame | None = None
    fleet_capacity: pd.DataFrame | None = None
    demand: pd.DataFrame | None = None
    supply: pd.DataFrame | None = None
    inventory_initial: pd.DataFrame | None = None
    inventory_in_transit: pd.DataFrame | None = None
    operation_capacities: pd.DataFrame | None = None
    operation_costs: pd.DataFrame | None = None
    transport_costs: pd.DataFrame | None = None
    resource_costs: pd.DataFrame | None = None
    commodity_sell_price_tiers: pd.DataFrame | None = None
    commodity_procurement_cost_tiers: pd.DataFrame | None = None
    facility_hierarchy_types: pd.DataFrame | None = None
    facility_hierarchy_levels: pd.DataFrame | None = None
    facility_hierarchy_nodes: pd.DataFrame | None = None
    facility_hierarchy_memberships: pd.DataFrame | None = None
    commodity_hierarchy_types: pd.DataFrame | None = None
    commodity_hierarchy_levels: pd.DataFrame | None = None
    commodity_hierarchy_nodes: pd.DataFrame | None = None
    commodity_hierarchy_memberships: pd.DataFrame | None = None
    scenarios: pd.DataFrame | None = None
    scenario_edge_rules: pd.DataFrame | None = None
    scenario_manual_edges: pd.DataFrame | None = None
    scenario_parameter_overrides: pd.DataFrame | None = None

    facility_spines: dict[str, pd.DataFrame] | None = None
    edge_spines: dict[str, pd.DataFrame] | None = None
    resource_spines: dict[str, pd.DataFrame] | None = None

    _SCHEMAS: ClassVar[dict[str, type[BaseModel]]] = {
        **RawModelData._SCHEMAS,
        "edge_lead_time_resolved": EdgeLeadTimeResolved,
    }

    _REQUIRED: ClassVar[frozenset[str]] = RawModelData._REQUIRED

    def validate(self) -> None:
        """Check required tables exist and columns match row schemas."""
        errors: list[str] = []
        for f in fields(self):
            if f.name.startswith("_") or f.name not in self._SCHEMAS:
                continue
            df = getattr(self, f.name)
            if f.name in self._REQUIRED:
                if df is None:
                    errors.append(f"{f.name} is required but is None")
                    continue
                errors.extend(_validate_dataframe_columns(f.name, df, self._SCHEMAS[f.name]))
            else:
                if df is None:
                    continue
                errors.extend(_validate_dataframe_columns(f.name, df, self._SCHEMAS[f.name]))

        if errors:
            raise ValueError("ResolvedModelData validation failed: " + "; ".join(errors))
