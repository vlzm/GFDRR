"""Edge and edge-related attribute row schemas."""

from __future__ import annotations

import datetime as dt

from pydantic import BaseModel, ConfigDict, Field


class Edge(BaseModel):
    """One row of the edge table (PK: source_id, target_id, modal_type)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    target_id: str
    modal_type: str
    distance: float = Field(ge=0)
    lead_time_hours: float = Field(ge=0)
    reliability: float | None = Field(default=None, ge=0, le=1)


class EdgeCommodity(BaseModel):
    """Allowed commodity on an edge."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    target_id: str
    modal_type: str
    commodity_category: str
    enabled: bool = True


class EdgeCapacity(BaseModel):
    """Shared edge capacity over time (raw date)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    target_id: str
    modal_type: str
    date: dt.date
    capacity: float = Field(gt=0)


class EdgeCommodityCapacity(BaseModel):
    """Per-commodity min/max shipment on an edge (raw date)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    target_id: str
    modal_type: str
    commodity_category: str
    date: dt.date
    min_shipment: float | None = Field(default=None, ge=0)
    max_shipment: float = Field(gt=0)


class EdgeVehicle(BaseModel):
    """Discrete vehicle trips on an edge."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    target_id: str
    modal_type: str
    resource_category: str
    vehicle_capacity: float = Field(gt=0)
    max_vehicles_per_period: int | None = Field(default=None, ge=0)


class EdgeLeadTimeResolved(BaseModel):
    """Generated: lead time in periods from departure period (edge x period_id)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    target_id: str
    modal_type: str
    period_id: str
    lead_time_periods: int = Field(ge=0)
    arrival_period_id: str | None = None
