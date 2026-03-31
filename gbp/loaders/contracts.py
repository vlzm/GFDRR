"""Contracts for the loaders package.

Pydantic models for configuration.
Pandera schemas for source DataFrame validation.
"""

from __future__ import annotations

from typing import Literal

import pandera.pandas as pa
from pandera.typing import Series
from pydantic import BaseModel, Field


# =============================================================================
# Pydantic — Configuration
# =============================================================================


class GraphLoaderConfig(BaseModel):
    """Configuration for DataLoaderGraph."""

    distance_backend: Literal["haversine", "euclidean"] = "haversine"
    default_speed_kmh: float = Field(default=50.0, gt=0)
    build_edges: bool = True
    build_observations: bool = True


# =============================================================================
# Pandera — Source Data Schemas
# =============================================================================


class StationsSourceSchema(pa.DataFrameModel):
    """Validates the stations DataFrame from a data source."""

    node_id: Series[str] = pa.Field(unique=True, str_length={"min_value": 1})
    inventory_capacity: Series[int] = pa.Field(gt=0)
    lat: Series[float] = pa.Field(ge=-90, le=90)
    lon: Series[float] = pa.Field(ge=-180, le=180)

    class Config:
        strict = False
        coerce = True


class DepotsSourceSchema(pa.DataFrameModel):
    """Validates the depots DataFrame from a data source."""

    node_id: Series[str] = pa.Field(unique=True, str_length={"min_value": 1})
    lat: Series[float] = pa.Field(ge=-90, le=90)
    lon: Series[float] = pa.Field(ge=-180, le=180)

    class Config:
        strict = False
        coerce = True


class ResourcesSourceSchema(pa.DataFrameModel):
    """Validates the resources DataFrame from a data source."""

    resource_id: Series[str] = pa.Field(unique=True, str_length={"min_value": 1})
    capacity: Series[int] = pa.Field(gt=0)

    class Config:
        strict = False
        coerce = True


class TripsSourceSchema(pa.DataFrameModel):
    """Validates the trips DataFrame from a data source."""

    started_at: Series  # type: ignore[type-arg]  # datetime, validated by coercion
    start_station_id: Series[str] = pa.Field(str_length={"min_value": 1})
    end_station_id: Series[str] = pa.Field(str_length={"min_value": 1})

    class Config:
        strict = False
        coerce = True
