"""Define planning horizon, segments, and period row schemas."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict, Field

from gbp.core.enums import PeriodType


class PlanningHorizon(BaseModel):
    """Represent one row of the planning_horizon table."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    planning_horizon_id: str
    name: str
    start_date: date
    end_date: date


class PlanningHorizonSegment(BaseModel):
    """Represent one row of the planning_horizon_segment table."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    planning_horizon_id: str
    segment_index: int = Field(ge=0)
    start_date: date
    end_date: date
    period_type: PeriodType


class Period(BaseModel):
    """Represent one row of the period table."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    period_id: str
    planning_horizon_id: str
    segment_index: int = Field(ge=0)
    period_index: int = Field(ge=0)
    period_type: PeriodType
    start_date: date
    end_date: date
