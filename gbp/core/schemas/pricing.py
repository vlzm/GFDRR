"""Tiered commodity pricing row schemas."""

from __future__ import annotations

import datetime as dt

from pydantic import BaseModel, ConfigDict, Field


class CommoditySellPriceTier(BaseModel):
    """Volume-tiered sell price at a SINK facility (raw date)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    facility_id: str
    commodity_category: str
    date: dt.date
    tier_index: int = Field(ge=0)
    min_volume: float = Field(ge=0)
    max_volume: float | None = Field(default=None, ge=0)
    price_per_unit: float = Field(ge=0)


class CommodityProcurementCostTier(BaseModel):
    """Volume-tiered procurement cost at a SOURCE facility (raw date)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    facility_id: str
    commodity_category: str
    date: dt.date
    tier_index: int = Field(ge=0)
    min_volume: float = Field(ge=0)
    max_volume: float | None = Field(default=None, ge=0)
    cost_per_unit: float = Field(ge=0)
