"""Facility behavior and edge rule row schemas."""

from __future__ import annotations

import datetime as dt

from pydantic import BaseModel, ConfigDict, Field

from gbp.core.enums import FacilityRole


class FacilityRoleRecord(BaseModel):
    """One row of the facility_role table."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    facility_id: str
    role: FacilityRole


class FacilityOperation(BaseModel):
    """One row of the facility_operation table."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    facility_id: str
    operation_type: str
    enabled: bool = True


class FacilityAvailability(BaseModel):
    """One row of the facility_availability table (raw dates)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    facility_id: str
    date: dt.date
    available: bool = True
    capacity_factor: float | None = Field(default=None, ge=0)


class EdgeRule(BaseModel):
    """One row of the edge_rule table."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_type: str
    target_type: str
    commodity_category: str | None = None
    modal_type: str | None = None
    enabled: bool = True
