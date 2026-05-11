"""Structural protocols for the loaders package.

Defines ``GenericSourceProtocol`` (minimal interface for any data source),
``BikeShareSourceProtocol`` (bike-sharing specific), and
``GraphLoaderProtocol`` (graph loader contract).
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
    """Minimal interface for any data source that can produce DataFrames.

    Methods
    -------
    load_data()
        Fetch or generate all source DataFrames.
    get_dataframes()
        Return a name-to-DataFrame mapping of loaded tables.
    """

    def load_data(self) -> None:
        """Fetch or generate all source DataFrames."""
        ...

    def get_dataframes(self) -> dict[str, pd.DataFrame]:
        """Return a name-to-DataFrame mapping of loaded tables."""
        ...


# ---------------------------------------------------------------------------
# Bike-sharing domain
# ---------------------------------------------------------------------------

class BikeShareSourceProtocol(Protocol):
    """Bike-sharing data source protocol.

    ``df_stations`` and ``df_trips`` are required; the rest is optional so a
    minimal source can be as small as two DataFrames.

    Optional attributes may be set to ``None`` (or missing entirely -- loaders
    look them up via ``getattr(..., None)``).  When an optional source is
    absent the loader skips the corresponding build step and lets
    ``build_model`` derive defaults where possible.

    Attributes
    ----------
    df_stations : pd.DataFrame
        Required. Station locations.
    df_trips : pd.DataFrame
        Required. Trip records.
    df_depots : pd.DataFrame or None
        Depot locations.
    df_resources : pd.DataFrame or None
        Resource (truck) identifiers.
    df_station_capacities : pd.DataFrame or None
        Per-station per-commodity capacity.
    df_depot_capacities : pd.DataFrame or None
        Per-depot per-commodity capacity.
    df_resource_capacities : pd.DataFrame or None
        Per-resource capacity.
    timestamps : pd.DatetimeIndex or None
        Time index for the simulation horizon.
    inventory_initial : pd.DataFrame or None
        Starting inventory per facility per commodity.
    df_telemetry_ts : pd.DataFrame or None
        Station telemetry time series (GBFS-like).
    df_station_costs : pd.DataFrame or None
        Station fixed costs.
    df_depot_costs : pd.DataFrame or None
        Depot fixed costs.
    df_truck_rates : pd.DataFrame or None
        Per-truck cost rates.
    """

    df_stations: pd.DataFrame
    df_trips: pd.DataFrame

    df_depots: pd.DataFrame | None
    df_resources: pd.DataFrame | None
    df_station_capacities: pd.DataFrame | None
    df_depot_capacities: pd.DataFrame | None
    df_resource_capacities: pd.DataFrame | None
    timestamps: pd.DatetimeIndex | None
    inventory_initial: pd.DataFrame | None
    df_telemetry_ts: pd.DataFrame | None
    df_station_costs: pd.DataFrame | None
    df_depot_costs: pd.DataFrame | None
    df_truck_rates: pd.DataFrame | None

    def load_data(self) -> None:
        """Fetch or generate all source DataFrames."""
        ...


# ---------------------------------------------------------------------------
# Graph loader
# ---------------------------------------------------------------------------

class GraphLoaderProtocol(Protocol):
    """Build ``RawModelData`` from a data source.

    Consumers that need a ``ResolvedModelData`` must call
    ``gbp.build.pipeline.build_model(loader.raw)`` themselves.
    """

    @property
    def raw(self) -> RawModelData:
        """Return the assembled ``RawModelData``."""
        ...

    @property
    def source(self) -> BikeShareSourceProtocol:
        """Return the underlying data source."""
        ...

    @property
    def available_dates(self) -> pd.DatetimeIndex:
        """Return available dates from the source."""
        ...

    def load(self) -> RawModelData:
        """Load source data and assemble ``RawModelData``."""
        ...
