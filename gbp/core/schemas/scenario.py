"""Scenario configuration row schemas."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class Scenario(BaseModel):
    """One planning scenario referencing a planning horizon."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    scenario_id: str
    planning_horizon_id: str
    name: str
    description: str | None = None
    facility_hierarchy_type: str | None = None
    facility_aggregation_level: int | None = Field(default=None, ge=0)
    commodity_hierarchy_type: str | None = None
    commodity_aggregation_level: int | None = Field(default=None, ge=0)


class ScenarioEdgeRules(BaseModel):
    """Edge rules scoped to a scenario."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    scenario_id: str
    source_type: str
    target_type: str
    commodity_category: str | None = None
    modal_type: str | None = None
    enabled: bool = True


class ScenarioManualEdges(BaseModel):
    """Manual edge triples added for a scenario."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    scenario_id: str
    source_id: str
    target_id: str
    modal_type: str
    commodity_category: str


class ScenarioParameterOverrides(BaseModel):
    """Scalar overrides for named attributes on entities."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    scenario_id: str
    attribute_name: str
    entity_id: str
    override_value: float
