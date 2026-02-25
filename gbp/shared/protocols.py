from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

import pandas as pd

if TYPE_CHECKING:
    from .graph_model import GraphData


class DataSourceProtocol(Protocol):
    """Protocol for raw (possibly temporal) data sources."""

    df_stations: pd.DataFrame
    df_depots: pd.DataFrame
    df_resources: pd.DataFrame
    timestamps: pd.DatetimeIndex
    df_inventory_ts: pd.DataFrame

    def load_data(self) -> None: ...


class DataLoaderGraphProtocol(Protocol):
    """Protocol for temporal-graph data loaders."""

    @property
    def available_dates(self) -> pd.DatetimeIndex: ...

    def load_data(self) -> None: ...

    def get_snapshot(self, date: pd.Timestamp) -> GraphData: ...


class DataLoaderRebalancerProtocol(Protocol):
    """Protocol for rebalancer data loaders."""

    df_node_demand: pd.DataFrame
    data: dict | None

    def load_data(self, date: pd.Timestamp | None = ...) -> None: ...
