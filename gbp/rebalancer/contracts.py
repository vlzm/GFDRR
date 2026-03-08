"""Contracts and schemas for the rebalancer package."""

from __future__ import annotations

from typing import Protocol, TypedDict

import numpy as np
import pandera.pandas as pa
import pandas as pd
from pandera.typing import Series
from pydantic import BaseModel, Field


class RebalancerConfig(BaseModel):
    """Configuration for rebalancing workflow."""

    inventory_node_type: str = "station"
    depot_node_type: str = "depot"
    min_threshold: float = Field(default=0.3, ge=0.0, le=1.0)
    max_threshold: float = Field(default=0.7, ge=0.0, le=1.0)
    time_limit_seconds: int = Field(default=30, gt=0)


class NodeStateSchema(pa.DataFrameModel):
    """State of nodes used for demand computation."""

    node_id: Series[str] = pa.Field(str_length={"min_value": 1}, unique=True)
    latitude: Series[float] = pa.Field(ge=-90, le=90)
    longitude: Series[float] = pa.Field(ge=-180, le=180)
    quantity: Series[int] = pa.Field(ge=0)
    inventory_capacity: Series[int] = pa.Field(gt=0)

    class Config:
        strict = False
        coerce = True


class NodeDemandSchema(NodeStateSchema):
    """Node state extended with utilization and balance values."""

    utilization: Series[float] = pa.Field(ge=0)
    target_count: Series[float] = pa.Field(ge=0)
    balance: Series[float]


class SourcesSchema(NodeDemandSchema):
    """Nodes that can donate inventory to others."""

    excess: Series[int] = pa.Field(gt=0)


class DestinationsSchema(NodeDemandSchema):
    """Nodes that need incoming inventory."""

    deficit: Series[int] = pa.Field(gt=0)


class RouteSchema(pa.DataFrameModel):
    """Route output produced by the PDP solver."""

    resource_id: Series[int] = pa.Field(ge=0)
    step: Series[int] = pa.Field(ge=0)
    node_id: Series[str] = pa.Field(str_length={"min_value": 1})
    action: Series[str] = pa.Field(isin=["depot", "pickup", "delivery"])
    quantity: Series[int] = pa.Field(ge=0)
    cumulative_load: Series[int]

    class Config:
        strict = False
        coerce = True


class UpdatedInventorySchema(NodeDemandSchema):
    """Node-demand table enriched with post-route inventory updates."""

    old_quantity: Series[int] = pa.Field(ge=0)
    inventory_change: Series[int]
    new_utilization: Series[float] = pa.Field(ge=0)


class PdpPair(TypedDict):
    """One pickup-delivery movement in the PDP model."""

    pickup_node_id: str
    pickup_latitude: float
    pickup_longitude: float
    delivery_node_id: str
    delivery_latitude: float
    delivery_longitude: float
    quantity: int


class PdpModel(TypedDict):
    """Input model expected by OR-Tools PDP solver."""

    distance_matrix: np.ndarray
    demands: list[int]
    pickups_deliveries: list[tuple[int, int]]
    resource_capacities: list[int]
    num_resources: int
    depot: int
    node_ids: list[str]
    pairs: list[PdpPair]


class DataLoaderRebalancerProtocol(Protocol):
    """Protocol used by pipeline to orchestrate the loader."""

    df_node_demand: pd.DataFrame
    data: PdpModel | None

    def load_data(self, date: pd.Timestamp | None = None) -> None: ...
