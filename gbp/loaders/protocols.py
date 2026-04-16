"""Structural protocols for the loaders package.

GenericSourceProtocol  — minimal interface for any data source.
BikeShareSourceProtocol — bike-sharing specific: stations, depots, trips, etc.
GraphLoaderProtocol    — what any graph loader must expose.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

import pandas as pd

if TYPE_CHECKING:
    from gbp.core.model import RawModelData


# ---------------------------------------------------------------------------
# Generic (aspirational — for future domain-agnostic loaders)
# ---------------------------------------------------------------------------

class GenericSourceProtocol(Protocol):
    """Minimal interface: any data source that can produce DataFrames."""

    def load_data(self) -> None: ...

    def get_dataframes(self) -> dict[str, pd.DataFrame]: ...


# ---------------------------------------------------------------------------
# Bike-sharing domain
# ---------------------------------------------------------------------------

class BikeShareSourceProtocol(Protocol):
    """Bike-sharing data source — stations and trips are required; the rest
    is optional so a minimal source can be as small as ``df_stations`` +
    ``df_trips``.

    Optional attributes may be set to ``None`` (or missing entirely — loaders
    look them up via ``getattr(..., None)``).  When an optional source is
    absent the loader skips the corresponding build step and lets
    ``build_model`` derive defaults where possible.
    """

    df_stations: pd.DataFrame
    df_trips: pd.DataFrame

    df_depots: pd.DataFrame | None
    df_resources: pd.DataFrame | None
    df_station_capacities: pd.DataFrame | None
    df_depot_capacities: pd.DataFrame | None
    df_resource_capacities: pd.DataFrame | None
    timestamps: pd.DatetimeIndex | None
    df_inventory_ts: pd.DataFrame | None
    df_telemetry_ts: pd.DataFrame | None
    df_station_costs: pd.DataFrame | None
    df_depot_costs: pd.DataFrame | None
    df_truck_rates: pd.DataFrame | None

    def load_data(self) -> None: ...


# ---------------------------------------------------------------------------
# Graph loader
# ---------------------------------------------------------------------------

class GraphLoaderProtocol(Protocol):
    """Builds a ``RawModelData`` from a data source.

    Consumers that need a ``ResolvedModelData`` must call
    ``gbp.build.pipeline.build_model(loader.raw)`` themselves.
    """

    @property
    def raw(self) -> RawModelData: ...

    @property
    def source(self) -> BikeShareSourceProtocol: ...

    @property
    def available_dates(self) -> pd.DatetimeIndex: ...

    def load(self) -> RawModelData: ...
