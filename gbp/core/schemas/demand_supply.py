"""Demand, supply, and inventory boundary row schemas."""

from __future__ import annotations

import datetime as dt

from pydantic import BaseModel, ConfigDict, Field


class Demand(BaseModel):
    """Time-varying demand at a SINK facility (raw date)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    facility_id: str
    commodity_category: str
    date: dt.date
    quantity: float = Field(ge=0)
    min_order_quantity: float | None = Field(default=None, ge=0)


class Supply(BaseModel):
    """Time-varying supply at a SOURCE facility (raw date)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    facility_id: str
    commodity_category: str
    date: dt.date
    quantity: float = Field(ge=0)


class InventoryInitial(BaseModel):
    """Inventory at planning horizon start for STORAGE facilities."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    facility_id: str
    commodity_category: str
    quantity: float = Field(ge=0)


class InventoryInTransit(BaseModel):
    """Commodity in transit at horizon start."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    target_id: str
    modal_type: str
    commodity_category: str
    quantity: float = Field(ge=0)
    departure_date: dt.date
    expected_arrival_date: dt.date
