"""Define edge and edge-related attribute row schemas."""

from __future__ import annotations

import datetime as dt

from pydantic import BaseModel, ConfigDict, Field


class Edge(BaseModel):
    """Represent one row of the edge table (PK: source_id, target_id, modal_type)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    target_id: str
    modal_type: str
    distance: float = Field(ge=0)
    lead_time_hours: float = Field(ge=0)
    reliability: float | None = Field(default=None, ge=0, le=1)


class EdgeCommodity(BaseModel):
    """Represent an allowed commodity on an edge."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    target_id: str
    modal_type: str
    commodity_category: str
    enabled: bool = True


class EdgeCapacity(BaseModel):
    """Represent shared edge capacity over time (raw date)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    target_id: str
    modal_type: str
    date: dt.date
    capacity: float = Field(gt=0)


class EdgeCommodityCapacity(BaseModel):
    """Represent per-commodity min/max shipment on an edge (raw date)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    target_id: str
    modal_type: str
    commodity_category: str
    date: dt.date
    min_shipment: float | None = Field(default=None, ge=0)
    max_shipment: float = Field(gt=0)


class EdgeVehicle(BaseModel):
    """Represent discrete vehicle trips on an edge."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    target_id: str
    modal_type: str
    resource_category: str
    vehicle_capacity: float = Field(gt=0)
    max_vehicles_per_period: int | None = Field(default=None, ge=0)


class DistanceMatrix(BaseModel):
    """Represent pairwise distance and travel duration between facilities.

    Declarative fact table: the loader computes distances (haversine, OSRM, ...)
    and ``build_model()`` merges them onto materialized edges.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    target_id: str
    distance: float = Field(ge=0)
    duration: float = Field(ge=0)


class EdgeLeadTimeResolved(BaseModel):
    """Represent generated lead time in periods from departure period (edge x period_id)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    target_id: str
    modal_type: str
    period_id: str
    lead_time_periods: int = Field(ge=0)
    arrival_period_id: str | None = None
