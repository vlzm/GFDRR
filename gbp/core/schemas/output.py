"""Define optimizer and simulator output row schemas."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class SolutionFlow(BaseModel):
    """Represent planned flow from optimizer."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    scenario_id: str
    source_id: str
    target_id: str
    modal_type: str
    commodity_category: str
    period_id: str
    quantity: float = Field(ge=0)


class SolutionInventory(BaseModel):
    """Represent planned end-of-period inventory from optimizer."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    scenario_id: str
    facility_id: str
    commodity_category: str
    period_id: str
    quantity: float = Field(ge=0)


class SolutionUnmetDemand(BaseModel):
    """Represent shortfall vs demand from optimizer."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    scenario_id: str
    facility_id: str
    commodity_category: str
    period_id: str
    shortfall: float = Field(ge=0)


class SolutionMetadata(BaseModel):
    """Represent optimizer run metadata (one row per scenario)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    scenario_id: str
    solve_timestamp: datetime
    objective_value: float | None = None
    solve_time_seconds: float = Field(ge=0)
    solver_status: str
    gap: float | None = Field(default=None, ge=0)


class SimulationFlowLog(BaseModel):
    """Represent simulated flow per period."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    scenario_id: str
    source_id: str
    target_id: str
    modal_type: str
    commodity_category: str
    period_id: str
    quantity: float = Field(ge=0)


class SimulationInventoryLog(BaseModel):
    """Represent simulated end-of-period inventory."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    scenario_id: str
    facility_id: str
    commodity_category: str
    period_id: str
    quantity: float = Field(ge=0)


class SimulationResourceLog(BaseModel):
    """Represent simulated resource state per period."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    scenario_id: str
    resource_id: str | None = None
    resource_category: str
    period_id: str
    facility_id: str
    status: str
    trips_completed: int = Field(ge=0)


class SimulationMetadata(BaseModel):
    """Represent simulator run metadata (one row per scenario)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    scenario_id: str
    simulation_timestamp: datetime
    total_periods: int = Field(ge=0)
    total_cost: float | None = None
    unmet_demand_total: float = Field(ge=0)
    solver_type: str
