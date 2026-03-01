"""Pandera schemas for data contracts.

Graph-layer schemas validate the fundamental graph tables.
Rebalancer schemas validate the demand / routing pipeline.
"""

from __future__ import annotations

from typing import TypedDict

import numpy as np
import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series


# =============================================================================
# Graph Layer
# =============================================================================


class GraphNodesSchema(pa.DataFrameModel):
    """Schema for the graph nodes table."""

    node_id: Series[str] = pa.Field(unique=True, str_length={"min_value": 1})
    node_type: Series[str] = pa.Field(str_length={"min_value": 1})
    node_name: Series[str]
    lat: Series[float] = pa.Field(ge=-90, le=90)
    lon: Series[float] = pa.Field(ge=-180, le=180)

    class Config:
        strict = True
        coerce = True


class GraphEdgesSchema(pa.DataFrameModel):
    """Schema for the graph edges table."""

    source_id: Series[str] = pa.Field(str_length={"min_value": 1})
    target_id: Series[str] = pa.Field(str_length={"min_value": 1})
    edge_type: Series[str] = pa.Field(str_length={"min_value": 1})
    distance: Series[float] = pa.Field(ge=0)

    class Config:
        strict = True
        coerce = True


class GraphResourcesSchema(pa.DataFrameModel):
    """Schema for the graph resources table."""

    resource_id: Series[str] = pa.Field(unique=True, str_length={"min_value": 1})
    resource_type: Series[str] = pa.Field(str_length={"min_value": 1})
    resource_capacity: Series[int] = pa.Field(gt=0)

    class Config:
        strict = True
        coerce = True


class GraphCommoditiesSchema(pa.DataFrameModel):
    """Schema for the graph commodities table."""

    commodity_id: Series[str] = pa.Field(unique=True, str_length={"min_value": 1})
    commodity_name: Series[str]
    commodity_type: Series[str] = pa.Field(str_length={"min_value": 1})

    class Config:
        strict = True
        coerce = True


class GraphInventorySchema(pa.DataFrameModel):
    """Schema for the graph inventory table."""

    node_id: Series[str] = pa.Field(str_length={"min_value": 1})
    commodity_type: Series[str] = pa.Field(str_length={"min_value": 1})
    commodity_quantity: Series[int] = pa.Field(ge=0)
    inventory_capacity: Series[int] = pa.Field(gt=0)

    @pa.dataframe_check
    def quantity_within_capacity(cls, df: pd.DataFrame) -> pd.Series:
        """Ensure each node's quantity does not exceed its capacity."""
        return df["commodity_quantity"] <= df["inventory_capacity"]

    class Config:
        strict = True
        coerce = True


# =============================================================================
# Rebalancer Pipeline
# =============================================================================


class PdpModel(TypedDict):
    """Typed structure for the Pickup-and-Delivery problem model."""

    distance_matrix: np.ndarray
    demands: list[int]
    pickups_deliveries: list[tuple[int, int]]
    resource_capacities: list[int]
    num_resources: int
    depot: int
    node_ids: list[str]
    pairs: list[dict]


class NodeStateSchema(pa.DataFrameModel):
    """Schema for node inventory state before demand calculation."""

    node_id: Series[str] = pa.Field(unique=True, str_length={"min_value": 1})
    lat: Series[float] = pa.Field(ge=-90, le=90)
    lon: Series[float] = pa.Field(ge=-180, le=180)
    commodity_quantity: Series[int] = pa.Field(ge=0)
    inventory_capacity: Series[int] = pa.Field(gt=0)

    @pa.dataframe_check
    def quantity_within_capacity(cls, df: pd.DataFrame) -> pd.Series:
        """Ensure each node quantity does not exceed capacity."""
        return df["commodity_quantity"] <= df["inventory_capacity"]

    class Config:
        strict = True
        coerce = True


class NodeDemandSchema(pa.DataFrameModel):
    """Schema for demand-enriched node state."""

    node_id: Series[str] = pa.Field(unique=True, str_length={"min_value": 1})
    lat: Series[float] = pa.Field(ge=-90, le=90)
    lon: Series[float] = pa.Field(ge=-180, le=180)
    commodity_quantity: Series[int] = pa.Field(ge=0)
    inventory_capacity: Series[int] = pa.Field(gt=0)
    utilization: Series[float] = pa.Field(ge=0, le=1)
    target_count: Series[float] = pa.Field(ge=0)
    balance: Series[float]

    @pa.dataframe_check
    def quantity_within_capacity(cls, df: pd.DataFrame) -> pd.Series:
        """Ensure each node quantity does not exceed capacity."""
        return df["commodity_quantity"] <= df["inventory_capacity"]

    @pa.dataframe_check
    def balance_matches_formula(cls, df: pd.DataFrame) -> pd.Series:
        """Ensure balance matches quantity minus target."""
        return pd.Series(
            np.isclose(
                df["balance"],
                df["commodity_quantity"] - df["target_count"],
            ),
            index=df.index,
        )

    class Config:
        strict = True
        coerce = True


class SourcesSchema(pa.DataFrameModel):
    """Schema for source nodes selected for pickup."""

    node_id: Series[str] = pa.Field(unique=True, str_length={"min_value": 1})
    lat: Series[float] = pa.Field(ge=-90, le=90)
    lon: Series[float] = pa.Field(ge=-180, le=180)
    commodity_quantity: Series[int] = pa.Field(ge=0)
    inventory_capacity: Series[int] = pa.Field(gt=0)
    utilization: Series[float] = pa.Field(ge=0, le=1)
    target_count: Series[float] = pa.Field(ge=0)
    balance: Series[float]
    excess: Series[int] = pa.Field(gt=0)

    @pa.dataframe_check
    def quantity_within_capacity(cls, df: pd.DataFrame) -> pd.Series:
        """Ensure each node quantity does not exceed capacity."""
        return df["commodity_quantity"] <= df["inventory_capacity"]

    class Config:
        strict = True
        coerce = True


class DestinationsSchema(pa.DataFrameModel):
    """Schema for destination nodes selected for delivery."""

    node_id: Series[str] = pa.Field(unique=True, str_length={"min_value": 1})
    lat: Series[float] = pa.Field(ge=-90, le=90)
    lon: Series[float] = pa.Field(ge=-180, le=180)
    commodity_quantity: Series[int] = pa.Field(ge=0)
    inventory_capacity: Series[int] = pa.Field(gt=0)
    utilization: Series[float] = pa.Field(ge=0, le=1)
    target_count: Series[float] = pa.Field(ge=0)
    balance: Series[float]
    deficit: Series[int] = pa.Field(gt=0)

    @pa.dataframe_check
    def quantity_within_capacity(cls, df: pd.DataFrame) -> pd.Series:
        """Ensure each node quantity does not exceed capacity."""
        return df["commodity_quantity"] <= df["inventory_capacity"]

    class Config:
        strict = True
        coerce = True


class PairsSchema(pa.DataFrameModel):
    """Schema for pickup-delivery pairs passed to the PDP model."""

    pickup_node_id: Series[str] = pa.Field(str_length={"min_value": 1})
    pickup_lat: Series[float] = pa.Field(ge=-90, le=90)
    pickup_lon: Series[float] = pa.Field(ge=-180, le=180)
    delivery_node_id: Series[str] = pa.Field(str_length={"min_value": 1})
    delivery_lat: Series[float] = pa.Field(ge=-90, le=90)
    delivery_lon: Series[float] = pa.Field(ge=-180, le=180)
    quantity: Series[int] = pa.Field(gt=0)

    @pa.dataframe_check
    def pickup_and_delivery_differ(cls, df: pd.DataFrame) -> pd.Series:
        """Ensure pickup and delivery are not the same node."""
        return df["pickup_node_id"] != df["delivery_node_id"]

    class Config:
        strict = True
        coerce = True


class RouteSchema(pa.DataFrameModel):
    """Schema for formatted PDP route output."""

    resource_id: Series[int] = pa.Field(ge=0)
    step: Series[int] = pa.Field(ge=0)
    node_id: Series[str] = pa.Field(str_length={"min_value": 1})
    action: Series[str] = pa.Field(isin=["depot", "pickup", "delivery"])
    quantity: Series[int] = pa.Field(ge=0)
    cumulative_load: Series[int]

    class Config:
        strict = True
        coerce = True


class UpdatedInventorySchema(pa.DataFrameModel):
    """Schema for inventory after applying PDP route actions."""

    node_id: Series[str] = pa.Field(str_length={"min_value": 1})
    commodity_quantity: Series[int] = pa.Field(ge=0)
    inventory_capacity: Series[int] = pa.Field(gt=0)
    old_commodity_quantity: Series[int] = pa.Field(ge=0)
    inventory_change: Series[int]
    new_utilization: Series[float] = pa.Field(ge=0, le=1)

    @pa.dataframe_check
    def quantity_within_capacity(cls, df: pd.DataFrame) -> pd.Series:
        """Ensure post-update quantity does not exceed capacity."""
        return df["commodity_quantity"] <= df["inventory_capacity"]

    class Config:
        strict = False
        coerce = True
