"""Observed flow and inventory row schemas (historical data)."""

from __future__ import annotations

import datetime as dt

from pydantic import BaseModel, ConfigDict, Field


class ObservedFlow(BaseModel):
    """Historical commodity movement between facilities (raw date)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    target_id: str
    commodity_category: str
    date: dt.date
    quantity: float = Field(ge=0)
    quantity_unit: str
    modal_type: str | None = None
    resource_id: str | None = None


class ObservedInventory(BaseModel):
    """Historical inventory snapshot at a facility (raw date)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    facility_id: str
    commodity_category: str
    date: dt.date
    quantity: float = Field(ge=0)
    quantity_unit: str
