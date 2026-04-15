"""Container contracts for raw and resolved tabular model data.

Tables are organized into logical groups for navigation while keeping
a flat dataclass layout for backward compatibility.  Every group is
accessible as a ``dict[str, pd.DataFrame]`` via a property:

    >>> raw.entity_tables      # facilities, commodity_categories, ...
    >>> raw.temporal_tables     # planning_horizon, segments, periods
    >>> raw.flow_tables         # demand, supply, inventory_initial, ...
    >>> raw.table_summary()     # quick overview of what's populated

Groups (in reading order):
    entity       — what exists in the network (3 required + 2 optional)
    temporal     — planning horizon and period grid (3 required)
    behavior     — roles, operations, availability, edge rules (4, 3 req.)
    edge         — edge identity and attributes (5 optional)
    flow_data    — demand, supply, inventory (4 optional)
    observations  — historical flow and inventory (2 optional)
    transformation — N:M commodity conversion (3 optional)
    resource     — fleet, compatibility, availability (4 optional)
    hierarchy    — facility + commodity hierarchies (8 optional)
    scenario     — run configuration and overrides (4 optional)

Parametric data (costs, capacities, pricing) lives in ``attributes``
(``AttributeRegistry``), accessible via ``parameter_tables``.

ResolvedModelData adds:
    generated    — edge_lead_time_resolved, transformation_resolved, fleet_capacity
    spines       — assembled attribute spines per entity type
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from typing import TYPE_CHECKING, ClassVar

import pandas as pd
from pydantic import BaseModel

from gbp.core.attributes.registry import AttributeRegistry

if TYPE_CHECKING:
    from gbp.build.report import BuildReport

from gbp.core.schemas import (
    Commodity,
    CommodityCategory,
    DistanceMatrix,
    CommodityHierarchyLevel,
    CommodityHierarchyMembership,
    CommodityHierarchyNode,
    CommodityHierarchyType,
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
    ObservedFlow,
    ObservedInventory,
    Period,
    PlanningHorizon,
    PlanningHorizonSegment,
    Resource,
    ResourceAvailability,
    ResourceCategory,
    ResourceCommodityCompatibility,
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
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


def _collect_group(obj: object, field_names: list[str]) -> dict[str, pd.DataFrame]:
    """Return {name: df} for non-None DataFrames in *field_names*."""
    result: dict[str, pd.DataFrame] = {}
    for name in field_names:
        val = getattr(obj, name, None)
        if val is not None and isinstance(val, pd.DataFrame):
            result[name] = val
    return result


def _compact_repr(obj: object, groups: dict[str, list[str]], required: frozenset[str]) -> str:
    """One-line-per-table repr: ``ClassName(tables=N, rows=M)`` + detail lines."""
    cls_name = type(obj).__name__
    table_count = 0
    total_rows = 0
    detail_parts: list[str] = []

    for group_name, field_names in groups.items():
        items: list[str] = []
        for fname in field_names:
            val = getattr(obj, fname, None)
            if val is not None and isinstance(val, pd.DataFrame):
                table_count += 1
                total_rows += len(val)
                items.append(f"{fname}={len(val)}")
        if items:
            detail_parts.append(f"  {group_name}: {', '.join(items)}")

    # Include registry attribute count
    attrs = getattr(obj, "attributes", None)
    n_attrs = len(attrs) if attrs else 0
    if n_attrs:
        detail_parts.append(f"  attributes: {n_attrs} registered")

    header = f"{cls_name}(tables={table_count}, total_rows={total_rows})"
    if detail_parts:
        return header + "\n" + "\n".join(detail_parts)
    return header


def _compact_repr_html(
    obj: object, groups: dict[str, list[str]], required: frozenset[str],
) -> str:
    """HTML table for rich Jupyter display."""
    cls_name = type(obj).__name__
    rows_html: list[str] = []

    for group_name, field_names in groups.items():
        first_in_group = True
        group_size = sum(
            1 for f in field_names
            if getattr(obj, f, None) is not None and isinstance(getattr(obj, f), pd.DataFrame)
        )
        for fname in field_names:
            val = getattr(obj, fname, None)
            if val is not None and isinstance(val, pd.DataFrame):
                n_rows = len(val)
                cols = ", ".join(val.columns[:6])
                if len(val.columns) > 6:
                    cols += f", ... (+{len(val.columns) - 6})"
                tag = " *" if fname in required else ""
                group_cell = (
                    f'<td rowspan="{group_size}" '
                    f'style="vertical-align:top;font-weight:bold;background:#f0f0f0">'
                    f"{group_name}</td>"
                    if first_in_group else ""
                )
                rows_html.append(
                    f"<tr>{group_cell}"
                    f"<td>{fname}{tag}</td>"
                    f"<td style='text-align:right'>{n_rows}</td>"
                    f"<td><code style='font-size:0.85em'>{cols}</code></td>"
                    f"</tr>"
                )
                first_in_group = False

    attrs = getattr(obj, "attributes", None)
    n_attrs = len(attrs) if attrs else 0
    if n_attrs:
        rows_html.append(
            f"<tr><td style='font-weight:bold;background:#f0f0f0'>parameters</td>"
            f"<td colspan='3'>{n_attrs} attributes registered</td></tr>"
        )

    total_tables = sum(
        1 for g in groups.values() for f in g
        if getattr(obj, f, None) is not None and isinstance(getattr(obj, f), pd.DataFrame)
    )

    return (
        f"<div><strong>{cls_name}</strong> "
        f"<span style='color:#666'>({total_tables} tables)</span>"
        f"<table style='margin-top:4px;border-collapse:collapse;font-size:0.9em'>"
        f"<tr style='border-bottom:1px solid #ccc'>"
        f"<th>Group</th><th>Table</th><th>Rows</th><th>Columns</th></tr>"
        + "\n".join(rows_html)
        + "</table>"
        + "<span style='font-size:0.8em;color:#999'>* = required</span></div>"
    )


def _table_summary(obj: object, groups: dict[str, list[str]], required: frozenset[str]) -> str:
    """Human-readable overview: group → table (rows) or '—'."""
    lines: list[str] = []
    cls_name = type(obj).__name__
    lines.append(f"{cls_name} — table summary")
    lines.append("=" * len(lines[0]))

    for group_name, field_names in groups.items():
        lines.append(f"\n  {group_name}")
        lines.append(f"  {'─' * len(group_name)}")
        for fname in field_names:
            val = getattr(obj, fname, None)
            if val is not None and isinstance(val, pd.DataFrame):
                tag = " (required)" if fname in required else ""
                lines.append(f"    {fname}: {len(val)} rows{tag}")
            elif fname in required:
                lines.append(f"    {fname}: ✗ MISSING (required)")
            else:
                lines.append(f"    {fname}: —")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Shared mixin — properties, display, validation
# ---------------------------------------------------------------------------

class _ModelDataMixin:
    """Shared group-access properties, display, and validation logic.

    Both ``RawModelData`` and ``ResolvedModelData`` inherit this mixin.
    Subclasses must define ``_GROUPS``, ``_SCHEMAS``, ``_REQUIRED``,
    ``_NON_TABLE_FIELDS`` class variables and an ``attributes`` field.
    """

    _GROUPS: ClassVar[dict[str, list[str]]]
    _SCHEMAS: ClassVar[dict[str, type[BaseModel]]]
    _REQUIRED: ClassVar[frozenset[str]]
    _NON_TABLE_FIELDS: ClassVar[frozenset[str]]
    attributes: AttributeRegistry

    # ── group access properties ───────────────────────────────────────

    @property
    def entity_tables(self) -> dict[str, pd.DataFrame]:
        """Core entities: facilities, commodity/resource categories, L3 items."""
        return _collect_group(self, self._GROUPS["entity"])

    @property
    def temporal_tables(self) -> dict[str, pd.DataFrame]:
        """Planning horizon, segments, and generated periods."""
        return _collect_group(self, self._GROUPS["temporal"])

    @property
    def behavior_tables(self) -> dict[str, pd.DataFrame]:
        """Facility roles, operations, availability, edge generation rules."""
        return _collect_group(self, self._GROUPS["behavior"])

    @property
    def edge_tables(self) -> dict[str, pd.DataFrame]:
        """Edge identity, commodities, capacities, vehicles."""
        return _collect_group(self, self._GROUPS["edge"])

    @property
    def flow_tables(self) -> dict[str, pd.DataFrame]:
        """Demand, supply, and inventory data."""
        return _collect_group(self, self._GROUPS["flow_data"])

    @property
    def observation_tables(self) -> dict[str, pd.DataFrame]:
        """Observed (historical) flow and inventory data."""
        return _collect_group(self, self._GROUPS["observations"])

    @property
    def transformation_tables(self) -> dict[str, pd.DataFrame]:
        """N:M commodity transformation definitions."""
        return _collect_group(self, self._GROUPS["transformation"])

    @property
    def resource_tables(self) -> dict[str, pd.DataFrame]:
        """Resource fleet, compatibility, and availability."""
        return _collect_group(self, self._GROUPS["resource"])

    @property
    def parameter_tables(self) -> dict[str, pd.DataFrame]:
        """All registered parametric attribute tables."""
        return self.attributes.to_dict()

    @property
    def hierarchy_tables(self) -> dict[str, pd.DataFrame]:
        """Facility and commodity hierarchy trees."""
        return _collect_group(self, self._GROUPS["hierarchy"])

    @property
    def scenario_tables(self) -> dict[str, pd.DataFrame]:
        """Scenario configuration and overrides."""
        return _collect_group(self, self._GROUPS["scenario"])

    # ── navigation helpers ────────────────────────────────────────────

    @property
    def populated_tables(self) -> dict[str, pd.DataFrame]:
        """All non-None DataFrames: structural fields + registry attributes."""
        result: dict[str, pd.DataFrame] = {}
        for f in fields(self):
            if f.name.startswith("_") or f.name in self._NON_TABLE_FIELDS:
                continue
            val = getattr(self, f.name)
            if isinstance(val, pd.DataFrame):
                result[f.name] = val
        result.update(self.attributes.to_dict())
        return result

    def table_summary(self) -> str:
        """Human-readable overview of populated tables, grouped logically.

        Usage::

            print(model.table_summary())
        """
        summary = _table_summary(self, self._GROUPS, self._REQUIRED)
        if self.attributes:
            summary += "\n\n  parameters (AttributeRegistry)"
            summary += f"\n  {'─' * 31}"
            summary += "\n" + self.attributes.summary()
        else:
            summary += "\n\n  parameters (AttributeRegistry)"
            summary += f"\n  {'─' * 31}"
            summary += "\n    (no attributes registered)"
        return summary

    # ── display ─────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        return _compact_repr(self, self._GROUPS, self._REQUIRED)

    def _repr_html_(self) -> str:
        return _compact_repr_html(self, self._GROUPS, self._REQUIRED)

    # ── validation ────────────────────────────────────────────────────

    def validate(self) -> None:
        """Check required tables exist and columns match row schemas."""
        cls_name = type(self).__name__
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
            raise ValueError(f"{cls_name} validation failed: " + "; ".join(errors))


# ---------------------------------------------------------------------------
# Shared tabular base — 40 fields common to Raw and Resolved
# ---------------------------------------------------------------------------

@dataclass(kw_only=True)
class _TabularModelBase(_ModelDataMixin):
    """Fields, groups, schemas, and required-set shared by Raw and Resolved.

    Both ``RawModelData`` and ``ResolvedModelData`` hold the same 40 logical
    tables (facilities, edges, demand, etc.); the only structural difference
    is that ``ResolvedModelData`` also carries build artifacts (lead-time
    resolution, transformation, fleet capacity) and assembled spines.

    Using ``kw_only=True`` means required fields can be declared alongside
    their domain group without needing ``= None`` workarounds.
    """

    # ── entity: what exists in the network ────────────────────────────
    facilities: pd.DataFrame
    commodity_categories: pd.DataFrame | None = None
    resource_categories: pd.DataFrame | None = None
    commodities: pd.DataFrame | None = None
    resources: pd.DataFrame | None = None

    # ── temporal: planning horizon and period grid ────────────────────
    planning_horizon: pd.DataFrame
    planning_horizon_segments: pd.DataFrame
    periods: pd.DataFrame | None = None

    # ── behavior: roles, operations, rules ────────────────────────────
    facility_roles: pd.DataFrame | None = None
    facility_operations: pd.DataFrame
    facility_availability: pd.DataFrame | None = None
    edge_rules: pd.DataFrame

    # ── edge: identity and attributes ─────────────────────────────────
    edges: pd.DataFrame | None = None
    edge_commodities: pd.DataFrame | None = None
    edge_capacities: pd.DataFrame | None = None
    edge_commodity_capacities: pd.DataFrame | None = None
    edge_vehicles: pd.DataFrame | None = None
    distance_matrix: pd.DataFrame | None = None

    # ── flow data: demand, supply, inventory ──────────────────────────
    demand: pd.DataFrame | None = None
    supply: pd.DataFrame | None = None
    inventory_initial: pd.DataFrame | None = None
    inventory_in_transit: pd.DataFrame | None = None

    # ── observations: historical flow and inventory ───────────────────
    observed_flow: pd.DataFrame | None = None
    observed_inventory: pd.DataFrame | None = None

    # ── transformation: N:M commodity conversion ──────────────────────
    transformations: pd.DataFrame | None = None
    transformation_inputs: pd.DataFrame | None = None
    transformation_outputs: pd.DataFrame | None = None

    # ── resource: fleet, compatibility, availability ──────────────────
    resource_commodity_compatibility: pd.DataFrame | None = None
    resource_modal_compatibility: pd.DataFrame | None = None
    resource_fleet: pd.DataFrame | None = None
    resource_availability: pd.DataFrame | None = None

    # ── hierarchy: facility + commodity trees ─────────────────────────
    facility_hierarchy_types: pd.DataFrame | None = None
    facility_hierarchy_levels: pd.DataFrame | None = None
    facility_hierarchy_nodes: pd.DataFrame | None = None
    facility_hierarchy_memberships: pd.DataFrame | None = None
    commodity_hierarchy_types: pd.DataFrame | None = None
    commodity_hierarchy_levels: pd.DataFrame | None = None
    commodity_hierarchy_nodes: pd.DataFrame | None = None
    commodity_hierarchy_memberships: pd.DataFrame | None = None

    # ── scenario: run configuration ───────────────────────────────────
    scenarios: pd.DataFrame | None = None
    scenario_edge_rules: pd.DataFrame | None = None
    scenario_manual_edges: pd.DataFrame | None = None
    scenario_parameter_overrides: pd.DataFrame | None = None

    # ── parametric attribute system ───────────────────────────────────
    attributes: AttributeRegistry = field(default_factory=AttributeRegistry)

    # ── class-level metadata ──────────────────────────────────────────

    _NON_TABLE_FIELDS: ClassVar[frozenset[str]] = frozenset({"attributes"})

    _GROUPS: ClassVar[dict[str, list[str]]] = {
        "entity": [
            "facilities", "commodity_categories", "resource_categories",
            "commodities", "resources",
        ],
        "temporal": [
            "planning_horizon", "planning_horizon_segments", "periods",
        ],
        "behavior": [
            "facility_roles", "facility_operations",
            "facility_availability", "edge_rules",
        ],
        "edge": [
            "edges", "edge_commodities", "edge_capacities",
            "edge_commodity_capacities", "edge_vehicles", "distance_matrix",
        ],
        "flow_data": [
            "demand", "supply", "inventory_initial", "inventory_in_transit",
        ],
        "observations": [
            "observed_flow", "observed_inventory",
        ],
        "transformation": [
            "transformations", "transformation_inputs", "transformation_outputs",
        ],
        "resource": [
            "resource_commodity_compatibility", "resource_modal_compatibility",
            "resource_fleet", "resource_availability",
        ],
        "hierarchy": [
            "facility_hierarchy_types", "facility_hierarchy_levels",
            "facility_hierarchy_nodes", "facility_hierarchy_memberships",
            "commodity_hierarchy_types", "commodity_hierarchy_levels",
            "commodity_hierarchy_nodes", "commodity_hierarchy_memberships",
        ],
        "scenario": [
            "scenarios", "scenario_edge_rules",
            "scenario_manual_edges", "scenario_parameter_overrides",
        ],
    }

    _SCHEMAS: ClassVar[dict[str, type[BaseModel]]] = {
        "facilities": Facility,
        "commodity_categories": CommodityCategory,
        "resource_categories": ResourceCategory,
        "commodities": Commodity,
        "resources": Resource,
        "planning_horizon": PlanningHorizon,
        "planning_horizon_segments": PlanningHorizonSegment,
        "periods": Period,
        "facility_roles": FacilityRoleRecord,
        "facility_operations": FacilityOperation,
        "facility_availability": FacilityAvailability,
        "edge_rules": EdgeRule,
        "edges": Edge,
        "edge_commodities": EdgeCommodity,
        "edge_capacities": EdgeCapacity,
        "edge_commodity_capacities": EdgeCommodityCapacity,
        "edge_vehicles": EdgeVehicle,
        "distance_matrix": DistanceMatrix,
        "demand": Demand,
        "supply": Supply,
        "inventory_initial": InventoryInitial,
        "inventory_in_transit": InventoryInTransit,
        "observed_flow": ObservedFlow,
        "observed_inventory": ObservedInventory,
        "transformations": Transformation,
        "transformation_inputs": TransformationInput,
        "transformation_outputs": TransformationOutput,
        "resource_commodity_compatibility": ResourceCommodityCompatibility,
        "resource_modal_compatibility": ResourceModalCompatibility,
        "resource_fleet": ResourceFleet,
        "resource_availability": ResourceAvailability,
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

    _REQUIRED: ClassVar[frozenset[str]] = frozenset({
        "facilities",
        "planning_horizon",
        "planning_horizon_segments",
        "facility_operations",
        "edge_rules",
    })


# ---------------------------------------------------------------------------
# RawModelData
# ---------------------------------------------------------------------------

@dataclass(kw_only=True)
class RawModelData(_TabularModelBase):
    """Raw input tables keyed by ``date`` where time-varying (pre-resolution).

    Inherits all 40 tabular fields from ``_TabularModelBase``.  Access groups
    via properties (``entity_tables``, ``temporal_tables``, etc.) or call
    ``table_summary()`` for a quick overview.
    """


# ---------------------------------------------------------------------------
# ResolvedModelData
# ---------------------------------------------------------------------------

@dataclass(kw_only=True)
class ResolvedModelData(_TabularModelBase):
    """Tables after time resolution (``period_id``) plus generated artifacts.

    Inherits all 40 tabular fields from ``_TabularModelBase`` and adds:

    - **generated**: ``edge_lead_time_resolved``, ``transformation_resolved``,
      ``fleet_capacity`` — produced by ``build_model()``
    - **spines**: assembled attribute DataFrames per entity type
    """

    # ── generated by build_model() ────────────────────────────────────
    edge_lead_time_resolved: pd.DataFrame | None = None
    transformation_resolved: pd.DataFrame | None = None
    fleet_capacity: pd.DataFrame | None = None

    # ── assembled spines ──────────────────────────────────────────────
    facility_spines: dict[str, pd.DataFrame] | None = None
    edge_spines: dict[str, pd.DataFrame] | None = None
    resource_spines: dict[str, pd.DataFrame] | None = None

    # ── diagnostics ────────────────────────────────────────────────────
    build_report: BuildReport | None = None

    # ── class-level metadata ──────────────────────────────────────────

    _GROUPS: ClassVar[dict[str, list[str]]] = {
        **_TabularModelBase._GROUPS,
        "generated": [
            "edge_lead_time_resolved", "transformation_resolved",
            "fleet_capacity",
        ],
    }

    _SCHEMAS: ClassVar[dict[str, type[BaseModel]]] = {
        **_TabularModelBase._SCHEMAS,
        "edge_lead_time_resolved": EdgeLeadTimeResolved,
    }

    _NON_TABLE_FIELDS: ClassVar[frozenset[str]] = frozenset(
        {"attributes", "build_report"}
    )

    # ── Resolved-only group access properties ───────────────────────────

    @property
    def generated_tables(self) -> dict[str, pd.DataFrame]:
        """Tables produced by build_model(): lead times, transformations, fleet."""
        return _collect_group(self, self._GROUPS["generated"])

    @property
    def spine_tables(self) -> dict[str, dict[str, pd.DataFrame]]:
        """Assembled attribute spines per entity type."""
        result: dict[str, dict[str, pd.DataFrame]] = {}
        for name in ("facility_spines", "edge_spines", "resource_spines"):
            val = getattr(self, name, None)
            if val is not None:
                result[name] = val
        return result

    # ── factory ────────────────────────────────────────────────────────

    @classmethod
    def from_raw(
        cls,
        raw: RawModelData,
        *,
        periods: pd.DataFrame,
        resolved_time: dict[str, pd.DataFrame],
        resolved_attrs: AttributeRegistry,
        edges: pd.DataFrame | None,
        edge_commodities: pd.DataFrame | None,
        edge_lead_time_resolved: pd.DataFrame | None,
        transformation_resolved: pd.DataFrame | None,
        fleet_capacity: pd.DataFrame | None,
    ) -> ResolvedModelData:
        """Build ``ResolvedModelData`` from raw tables and build artifacts.

        Centralizes the ``raw.field → resolved.field`` mapping so that
        ``build_model()`` and notebooks share one canonical constructor call.

        Args:
            raw: Validated raw model data.
            periods: Period grid (possibly modified copy of ``raw.periods``).
            resolved_time: Time-resolved structural tables from
                ``resolve_all_time_varying``.
            resolved_attrs: Registry with ``date → period_id`` resolved.
            edges: Edge table (from raw or ``build_edges``).
            edge_commodities: Edge-commodity table (from raw or built).
            edge_lead_time_resolved: Lead-time resolution output.
            transformation_resolved: N:M transformation output.
            fleet_capacity: Fleet capacity output.
        """

        def _coalesce(key: str, raw_df: pd.DataFrame | None) -> pd.DataFrame | None:
            """Prefer time-resolved table when present; otherwise pass through raw."""
            if key in resolved_time and not resolved_time[key].empty:
                return resolved_time[key]
            return raw_df

        return cls(
            facilities=raw.facilities,
            commodity_categories=raw.commodity_categories,
            resource_categories=raw.resource_categories,
            planning_horizon=raw.planning_horizon,
            planning_horizon_segments=raw.planning_horizon_segments,
            periods=periods,
            facility_roles=raw.facility_roles,
            facility_operations=raw.facility_operations,
            edge_rules=raw.edge_rules,
            resources=raw.resources,
            commodities=raw.commodities,
            facility_availability=_coalesce(
                "facility_availability", raw.facility_availability,
            ),
            transformations=raw.transformations,
            transformation_inputs=raw.transformation_inputs,
            transformation_outputs=raw.transformation_outputs,
            resource_commodity_compatibility=raw.resource_commodity_compatibility,
            resource_modal_compatibility=raw.resource_modal_compatibility,
            resource_fleet=raw.resource_fleet,
            resource_availability=raw.resource_availability,
            edges=edges,
            edge_commodities=(
                edge_commodities if edge_commodities is not None
                else raw.edge_commodities
            ),
            edge_capacities=_coalesce("edge_capacities", raw.edge_capacities),
            edge_commodity_capacities=_coalesce(
                "edge_commodity_capacities", raw.edge_commodity_capacities,
            ),
            edge_vehicles=raw.edge_vehicles,
            distance_matrix=raw.distance_matrix,
            edge_lead_time_resolved=edge_lead_time_resolved,
            transformation_resolved=transformation_resolved,
            fleet_capacity=fleet_capacity,
            demand=_coalesce("demand", raw.demand),
            supply=_coalesce("supply", raw.supply),
            inventory_initial=raw.inventory_initial,
            inventory_in_transit=raw.inventory_in_transit,
            observed_flow=_coalesce("observed_flow", raw.observed_flow),
            observed_inventory=_coalesce(
                "observed_inventory", raw.observed_inventory,
            ),
            facility_hierarchy_types=raw.facility_hierarchy_types,
            facility_hierarchy_levels=raw.facility_hierarchy_levels,
            facility_hierarchy_nodes=raw.facility_hierarchy_nodes,
            facility_hierarchy_memberships=raw.facility_hierarchy_memberships,
            commodity_hierarchy_types=raw.commodity_hierarchy_types,
            commodity_hierarchy_levels=raw.commodity_hierarchy_levels,
            commodity_hierarchy_nodes=raw.commodity_hierarchy_nodes,
            commodity_hierarchy_memberships=raw.commodity_hierarchy_memberships,
            scenarios=raw.scenarios,
            scenario_edge_rules=raw.scenario_edge_rules,
            scenario_manual_edges=raw.scenario_manual_edges,
            scenario_parameter_overrides=raw.scenario_parameter_overrides,
            attributes=resolved_attrs,
        )

