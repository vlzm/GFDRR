"""Define resource compatibility, fleet, and availability row schemas."""

from __future__ import annotations

import datetime as dt

from pydantic import BaseModel, ConfigDict, Field


class ResourceCommodityCompatibility(BaseModel):
    """Define which commodities a resource category can carry."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    resource_category: str
    commodity_category: str
    enabled: bool = True


class ResourceModalCompatibility(BaseModel):
    """Define on which modal types a resource category can operate."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    resource_category: str
    modal_type: str
    enabled: bool = True


class ResourceFleet(BaseModel):
    """Represent aggregated resource count at a facility (home base)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    facility_id: str
    resource_category: str
    count: int = Field(ge=0)


class ResourceAvailability(BaseModel):
    """Represent per-resource availability (L3, optional)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    resource_id: str
    date: dt.date
    available: bool = True
    available_capacity: float | None = Field(default=None, ge=0)
