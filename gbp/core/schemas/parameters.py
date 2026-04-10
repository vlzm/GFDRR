"""Operation, transport, and resource cost row schemas."""

from __future__ import annotations

import datetime as dt

from pydantic import BaseModel, ConfigDict, Field


class OperationCapacity(BaseModel):
    """Capacity limit for an operation at a facility."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    facility_id: str
    operation_type: str
    commodity_category: str | None = None
    capacity: float = Field(gt=0)


class OperationCost(BaseModel):
    """Time-varying operation cost (raw date)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    facility_id: str
    operation_type: str
    commodity_category: str
    date: dt.date
    cost_per_unit: float = Field(ge=0)


class TransportCost(BaseModel):
    """Time-varying transport cost on an edge (raw date)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    target_id: str
    modal_type: str
    resource_category: str
    date: dt.date
    cost_per_unit: float = Field(ge=0)


class ResourceCost(BaseModel):
    """EAV-style custom resource attribute (cost or rate)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    resource_category: str
    facility_id: str | None = None
    attribute_name: str
    date: dt.date | None = None
    value: float
