"""The scenario inputs: the input tables of one scenario, named in one place."""

from typing import Protocol

import pandas as pd

from gbp.routing import Routes


class ScenarioInputs(Protocol):
    """The input tables of one scenario."""

    # Read by every run: the engine, the canonical phases, the validator.
    #: The period grid: one row per ``period_id`` with its wall-clock bounds.
    periods_df: pd.DataFrame
    #: Starting inventory: one row per ``(facility_id, commodity_category)``.
    initial_inventory_df: pd.DataFrame
    #: The demand the run faces: quantity per (period, facility, commodity).
    #: A forecast run puts the forecast demand table here (Notations.md §11).
    historical_demand_df: pd.DataFrame
    #: The OD matrix: target probability and mean duration per source pair.
    historical_od_matrix_df: pd.DataFrame
    #: Dock capacities: one row per facility.
    facilities_capacities_df: pd.DataFrame
    #: Facility coordinates; the redirect's nearest-station search reads them.
    facilities_geo_df: pd.DataFrame
    #: The one distance / travel-time answerer for facility pairs.
    routes: Routes

    # Read only by the sizing run (``size_state_for_demand``).
    #: Facilities with their category (station or depot).
    facilities_df: pd.DataFrame
    #: The commodity categories (bike types).
    commodities_categories_df: pd.DataFrame

    # Read only by the rebalancing phases (Notations.md §14).
    #: Historical arrivals marginal; the target inventory is computed from it.
    historical_arrivals_df: pd.DataFrame
    #: The trucks, each with its home depot.
    resources_df: pd.DataFrame
    #: Truck capacities: one row per resource.
    resources_capacities_df: pd.DataFrame
    #: Length of a single simulation period.
    period_len: pd.Timedelta
