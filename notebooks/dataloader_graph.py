"""Graph (resolved) model data: the period grid, the historical flow log, the
graph entities/attributes, and the replay demand the engine consumes.

``ResolvedModelData`` is built once per scenario from a :class:`RawModelData`
and exposes the graph tables. The :class:`~engine.Environment` reads a narrow
subset of them: ``periods_df``, ``initial_inventory_df``, ``potential_trips_df``,
``facilities_capacities_df`` and ``facilities_geo_df``.
"""

import pandas as pd

from dataloader_raw import RawModelData, get_initial_inventory_df
from state import (
    FLOW_EVENT_COLUMNS,
    FLOW_EVENT_DTYPES,
    flows_to_arrivals,
    flows_to_departures,
    flows_to_od_matrix,
    get_inventory_df,
)


# ---------------------------------------------------------------------------
# Period grid (the simulation clock)
# ---------------------------------------------------------------------------
def to_period_id(ts: pd.Series, t0: pd.Timestamp, period_len: pd.Timedelta) -> pd.Series:
    return ((ts - t0) // period_len).astype("int64")


def get_periods_df(trips_df: pd.DataFrame, t0: pd.Timestamp, period_len: pd.Timedelta) -> pd.DataFrame:
    n_periods = int(to_period_id(trips_df["ended_at"], t0, period_len).max()) + 1
    periods_df = pd.DataFrame({"period_id": range(n_periods)})
    periods_df["start_timestamp"] = t0 + periods_df["period_id"] * period_len
    periods_df["end_timestamp"] = periods_df["start_timestamp"] + period_len
    return periods_df


# ---------------------------------------------------------------------------
# Flow event log
# ---------------------------------------------------------------------------
def get_historical_flows_df(trips_df: pd.DataFrame, t0: pd.Timestamp, period_len: pd.Timedelta) -> pd.DataFrame:
    """Expand each historical trip into a realized-flow event log.

    Ground-truth history contains only flows that actually happened, so each
    completed trip is one flow that emits two events in order: ``departed`` at
    the start period and ``arrived`` at the end period. The lifecycle states
    that exist only under simulation (``requested``, ``lost``, ``redirected``,
    ``cancelled``) are intentionally absent here — this is a representation of
    input data, not the simulator's own journal. Each field is filled only by
    the event that determines it (e.g. ``realized_end_period`` is null until
    ``arrived``); the full picture of a flow is recovered by stitching its rows
    on ``flow_id``.

    ``flow_id`` is namespaced with a ``hist_`` prefix so it cannot collide with
    flows the simulator generates and appends to the same journal.

    Parameters
    ----------
    trips_df : pandas.DataFrame
        Trips with ``started_at``, ``ended_at``, ``start_station_id``,
        ``end_station_id`` and ``rideable_type``. The row index seeds ``flow_id``.
    t0 : pandas.Timestamp
        Origin of the period grid.
    period_len : pandas.Timedelta
        Length of a single period.

    Returns
    -------
    pandas.DataFrame
        Event log with columns :data:`FLOW_EVENT_COLUMNS`, sorted by
        ``period_id`` then ``flow_id``, with a monotonic ``event_id``.
    """
    base = trips_df.rename(columns={
        "start_station_id": "source_id",
        "end_station_id":   "planned_target_id",
        "rideable_type":    "commodity_category",
    })[["source_id", "planned_target_id", "commodity_category"]].copy()
    base["flow_id"]     = "hist_" + base.index.astype("string")
    base["flow_type"]   = "user_trip"
    base["resource_id"] = pd.NA   # user trips are not carried by a truck
    base["quantity"]    = 1
    base["reason"]      = pd.NA

    sp = to_period_id(trips_df["started_at"], t0, period_len)
    ep = to_period_id(trips_df["ended_at"], t0, period_len)

    carried = ["flow_id", "flow_type", "commodity_category", "source_id",
               "planned_target_id", "resource_id", "quantity", "reason"]

    departed = base[carried].assign(
        event_type="departed", period_id=sp, event_order=0,
        realized_target_id=pd.NA,
        start_period=sp, planned_end_period=ep, realized_end_period=pd.NA,
    )
    arrived = base[carried].assign(
        event_type="arrived", period_id=ep, event_order=1,
        realized_target_id=base["planned_target_id"],
        start_period=sp, planned_end_period=ep, realized_end_period=ep,
    )

    blocks = [b.astype(FLOW_EVENT_DTYPES) for b in (departed, arrived)]
    flows = pd.concat(blocks, ignore_index=True)
    flows = flows.sort_values(["period_id", "flow_id", "event_order"]).reset_index(drop=True)
    flows["event_id"] = flows.index.astype("Int64")
    return flows[FLOW_EVENT_COLUMNS]


# ---------------------------------------------------------------------------
# Entity definitions
# ---------------------------------------------------------------------------
def get_facilities_df(stations_df: pd.DataFrame, depots_df: pd.DataFrame) -> pd.DataFrame:
    return pd.concat([
        stations_df[['station_id']].rename(columns={"station_id": "facility_id"}).assign(facility_category="station"),
        depots_df[['depot_id']].rename(columns={"depot_id": "facility_id"}).assign(facility_category="depot"),
    ], ignore_index=True)


def get_resources_df(trucks_df: pd.DataFrame) -> pd.DataFrame:
    return pd.concat([
        trucks_df[['truck_id']].rename(columns={"truck_id": "resource_id"}).assign(resource_category="truck"),
    ], ignore_index=True)


def get_commodities_categories_df() -> pd.DataFrame:
    return pd.DataFrame({
        "commodity_category": ["classic_bike", "electric_bike"],
    })


def get_facilities_geo_df(stations_df: pd.DataFrame, depots_df: pd.DataFrame) -> pd.DataFrame:
    """Geographical attributes: facility_id, lat, lng."""
    return pd.concat([
        stations_df[["station_id", "lat", "lng"]].rename(columns={"station_id": "facility_id"}),
        depots_df[["depot_id", "lat", "lng"]].rename(columns={"depot_id": "facility_id"}),
    ], ignore_index=True)


def get_facilities_capacities_df(stations_capacities_df: pd.DataFrame, depot_capacities_df: pd.DataFrame) -> pd.DataFrame:
    """Capacities: facility_id, capacity."""
    return pd.concat([
        stations_capacities_df.rename(columns={"station_id": "facility_id"}),
        depot_capacities_df.rename(columns={"depot_id": "facility_id"}),
    ], ignore_index=True)


def get_resources_capacities_df(trucks_capacities_df: pd.DataFrame) -> pd.DataFrame:
    """resource_id, capacity."""
    return trucks_capacities_df.rename(columns={"truck_id": "resource_id"})


def get_facilities_costs_df(stations_costs_df: pd.DataFrame, depot_costs_df: pd.DataFrame) -> pd.DataFrame:
    """Costs: facility_id, fixed_cost."""
    return pd.concat([
        stations_costs_df.rename(columns={"station_id": "facility_id", "fixed_cost_station": "fixed_cost"}),
        depot_costs_df.rename(columns={"depot_id": "facility_id", "fixed_cost_depot": "fixed_cost"}),
    ], ignore_index=True)


def get_resources_rates_df(trucks_rates_df: pd.DataFrame) -> pd.DataFrame:
    return trucks_rates_df.rename(columns={"truck_id": "resource_id"})


def get_commodities_categories_rates_df(bike_rates_df: pd.DataFrame) -> pd.DataFrame:
    return bike_rates_df.rename(columns={"rideable_type": "commodity_category"})


def get_resources_additional_attributes_df(trucks_df: pd.DataFrame) -> pd.DataFrame:
    """Additional attributes: resource_id, home_facility_id."""
    return (
        trucks_df.rename(columns={"truck_id": "resource_id"}).assign(home_facility_id="depot_1")
    )


# ---------------------------------------------------------------------------
# Resource observations
# ---------------------------------------------------------------------------
RESOURCE_OBS_COLUMNS = ["period_id", "resource_id", "facility_id", "load"]


def empty_resources_obs_df() -> pd.DataFrame:
    """Empty resource-observation table (trucks are idle in the replay)."""
    return pd.DataFrame({
        "period_id":   pd.Series(dtype="Int64"),
        "resource_id": pd.Series(dtype="string"),
        "facility_id": pd.Series(dtype="string"),
        "load":        pd.Series(dtype="Int64"),
    })


# ---------------------------------------------------------------------------
# Replay demand
# ---------------------------------------------------------------------------
def build_potential_trips(historical_flows_df: pd.DataFrame) -> pd.DataFrame:
    """Replay demand: one concrete desired trip per historical departure.

    Carries the real target and duration of every trip, which is what makes the
    base run reproduce history exactly instead of resampling it.
    """
    cols = ["flow_id", "source_id", "planned_target_id",
            "commodity_category", "start_period", "planned_end_period"]
    return historical_flows_df.query("event_type == 'departed'")[cols].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Resolved model data container
# ---------------------------------------------------------------------------
class ResolvedModelData:
    """Graph data for one scenario, built from a :class:`RawModelData`.

    Exposes the rich graph tables (entities, attributes, historical
    observations). The engine reads directly: ``periods_df``,
    ``initial_inventory_df``, ``potential_trips_df``,
    ``facilities_capacities_df`` and ``facilities_geo_df``.

    Parameters
    ----------
    raw : RawModelData
        The loaded raw entity tables.
    period_len : pandas.Timedelta, optional
        Length of a single simulation period. Defaults to one hour.
    """

    def __init__(self, raw: RawModelData, period_len: pd.Timedelta = pd.Timedelta(hours=1)) -> None:
        # Entities
        self.facilities_df = get_facilities_df(raw.stations_df, raw.depots_df)
        self.resources_df = get_resources_df(raw.trucks_df)
        self.commodities_categories_df = get_commodities_categories_df()

        # Attributes
        self.facilities_geo_df = get_facilities_geo_df(raw.stations_df, raw.depots_df)
        self.facilities_capacities_df = get_facilities_capacities_df(
            raw.stations_capacities_df, raw.depot_capacities_df
        )
        self.resources_capacities_df = get_resources_capacities_df(raw.trucks_capacities_df)
        self.facilities_costs_df = get_facilities_costs_df(raw.stations_costs_df, raw.depot_costs_df)
        self.resources_rates_df = get_resources_rates_df(raw.trucks_rates_df)
        self.commodities_categories_rates_df = get_commodities_categories_rates_df(raw.bike_rates_df)

        # Time grid
        self.period_len = period_len
        self.t0 = raw.trips_df["started_at"].min().floor("h")
        self.periods_df = get_periods_df(raw.trips_df, self.t0, period_len)

        # Initial inventory
        self.initial_inventory_df = get_initial_inventory_df(raw.gbfs_raw_df, raw.stations_df)

        # Historical observations -- general
        self.historical_flows_df = get_historical_flows_df(raw.trips_df, self.t0, period_len)
        self.historical_resources_df = empty_resources_obs_df()
        self.historical_inventory_df = get_inventory_df(self.historical_flows_df, self.initial_inventory_df)
        self.historical_demand_df = flows_to_departures(self.historical_flows_df)

        # Historical observations -- additional (marginals of the flow log)
        self.historical_departures_df = flows_to_departures(self.historical_flows_df)
        self.historical_arrivals_df = flows_to_arrivals(self.historical_flows_df)
        self.historical_od_matrix_df = flows_to_od_matrix(self.historical_flows_df)

        # Simulated observations -- filled by attach_simulation after a run
        self.simulated_flows_df: pd.DataFrame | None = None
        self.simulated_resources_df: pd.DataFrame | None = None
        self.simulated_inventory_df: pd.DataFrame | None = None
        self.simulated_demand_df: pd.DataFrame | None = None
        self.simulated_departures_df: pd.DataFrame | None = None
        self.simulated_arrivals_df: pd.DataFrame | None = None
        self.simulated_od_matrix_df: pd.DataFrame | None = None

        # Replay demand: one concrete desired trip per historical departure
        self.potential_trips_df = build_potential_trips(self.historical_flows_df)


# ---------------------------------------------------------------------------
# Wiring a finished run back into the resolved container
# ---------------------------------------------------------------------------
def attach_simulation(
    resolved: ResolvedModelData,
    simulated_flows_df: pd.DataFrame,
    simulated_resources_df: pd.DataFrame | None = None,
) -> None:
    """Populate the ``simulated_*`` observation slots from a finished run.

    All simulated marginals are derived from the finalized flow journal with the
    same functions used for the historical ones, so the two sets are directly
    comparable (in the base scenario they are equal).

    Parameters
    ----------
    resolved : ResolvedModelData
        Container to fill in place.
    simulated_flows_df : pandas.DataFrame
        Finalized simulated flow journal (e.g. ``Environment.simulated_flows_df``).
    simulated_resources_df : pandas.DataFrame, optional
        Resource observations from the run. Defaults to an empty table (trucks
        are idle in the historical replay).
    """
    resolved.simulated_flows_df = simulated_flows_df
    resolved.simulated_resources_df = (
        simulated_resources_df if simulated_resources_df is not None else empty_resources_obs_df()
    )
    resolved.simulated_inventory_df = get_inventory_df(simulated_flows_df, resolved.initial_inventory_df)
    resolved.simulated_demand_df = flows_to_departures(simulated_flows_df)
    resolved.simulated_departures_df = flows_to_departures(simulated_flows_df)
    resolved.simulated_arrivals_df = flows_to_arrivals(simulated_flows_df)
    resolved.simulated_od_matrix_df = flows_to_od_matrix(simulated_flows_df)
