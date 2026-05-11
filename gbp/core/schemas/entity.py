"""Define entity table row schemas (facilities, commodities, resources)."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class Facility(BaseModel):
    """Represent one row of the facility table."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    facility_id: str
    facility_type: str
    name: str
    lat: float | None = None
    lon: float | None = None


class CommodityCategory(BaseModel):
    """Represent one row of the commodity_category table."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    commodity_category_id: str
    name: str
    unit: str
    description: str | None = None


class ResourceCategory(BaseModel):
    """Represent one row of the resource_category table."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    resource_category_id: str
    name: str
    base_capacity: float = Field(ge=0)
    description: str | None = None


class Resource(BaseModel):
    """Represent one row of the resource table (L3, optional instance-level resource)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    resource_id: str
    resource_category: str
    home_facility_id: str
    capacity_override: float | None = Field(default=None, ge=0)
    description: str | None = None


class Commodity(BaseModel):
    """Represent one row of the commodity table (L3, optional instance-level commodity)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    commodity_id: str
    commodity_category: str
    description: str | None = None
