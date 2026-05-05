"""Observed flow and inventory row schemas (historical data)."""

from __future__ import annotations

import datetime as dt

from pydantic import BaseModel, ConfigDict, Field


class ObservedFlow(BaseModel):
    """Historical commodity movement between facilities (raw date).

    The optional ``duration_hours`` carries the absolute trip duration in hours
    used by the historical-replay pipeline to compute the arrival period
    (``arrival_period = period_index + ceil(duration_hours / period_duration_hours)``).
    ``None`` means same-period delivery (legacy fallback for sources that do not
    record trip end times).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    target_id: str
    commodity_category: str
    date: dt.date
    quantity: float = Field(ge=0)
    duration_hours: float | None = Field(default=None, ge=0.0)
    modal_type: str | None = None
    resource_id: str | None = None


class ObservedInventory(BaseModel):
    """Historical inventory snapshot at a facility (raw date)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    facility_id: str
    commodity_category: str
    date: dt.date
    quantity: float = Field(ge=0)
