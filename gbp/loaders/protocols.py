"""Structural protocols for the loaders package.

DataSourceProtocol — what any raw data source must expose.
GraphLoaderProtocol — what any graph loader must expose.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

import pandas as pd

if TYPE_CHECKING:
    from gbp.graph.core import GraphData


class DataSourceProtocol(Protocol):
    """Raw (possibly temporal) data source — mock, CSV, DB, API, etc."""

    df_stations: pd.DataFrame
    df_depots: pd.DataFrame
    df_resources: pd.DataFrame
    timestamps: pd.DatetimeIndex
    df_inventory_ts: pd.DataFrame

    def load_data(self) -> None: ...


class GraphLoaderProtocol(Protocol):
    """Temporal-graph data loader."""

    @property
    def available_dates(self) -> pd.DatetimeIndex: ...

    def load_data(self) -> None: ...

    def get_snapshot(self, date: pd.Timestamp) -> GraphData: ...
