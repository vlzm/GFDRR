"""Turn the raw Citi Bike tables into the framework's resolved model data."""

import copy

import pandas as pd

from gbp.model import arrived_events, departed_events, finalize_flows, haversine_km
from gbp.model.dataloader_graph import (
    DEFAULT_PERIOD_LEN,
    PeriodGrid,
    ResolvedModelData,
    to_period_id,
)
from gbp.model.flows import stamp_history_ordering
from gbp.model.journal_schema import check_journal_schema
from gbp.routing import DEFAULT_OSRM_URL, RoutingMode

from .dataloader_raw import (
    RawModelData,
    get_trucks_capacities_df,
    get_trucks_rates_df,
)


# ---------------------------------------------------------------------------
# Period grid
# ---------------------------------------------------------------------------
def get_periods_df(
    trips_df: pd.DataFrame, t0: pd.Timestamp, period_len: pd.Timedelta
) -> pd.DataFrame:
    """Build the period grid covering every trip, with start/end timestamps."""
    n_periods = int(to_period_id(trips_df["ended_at"], t0, period_len).max()) + 1
    return PeriodGrid(t0, n_periods, period_len).frame()


# ---------------------------------------------------------------------------
# Flow event log
# ---------------------------------------------------------------------------
def get_historical_flows_df(
    trips_df: pd.DataFrame, t0: pd.Timestamp, period_len: pd.Timedelta
) -> pd.DataFrame:
    """Expand each historical trip into a realized-flow event log, checked against the schema."""
    trips = pd.DataFrame(
        {
            "flow_id": "hist_" + trips_df.index.astype("string"),
            "source_id": trips_df["start_station_id"],
            "planned_target_id": trips_df["end_station_id"],
            "commodity_category": trips_df["rideable_type"],
            "start_period": to_period_id(trips_df["started_at"], t0, period_len),
            "planned_end_period": to_period_id(trips_df["ended_at"], t0, period_len),
        }
    )
    departed = departed_events(trips)
    arrived = arrived_events(trips, trips["planned_end_period"])
    journal = pd.concat([departed, arrived], ignore_index=True)
    # The loader has no phases, so nothing opened inventory steps. Stamp the
    # ordering columns with the history rule instead (phase_rank by timing,
    # step_id numbers the (period_id, phase_rank, phase_round) labels). The
    # simulator stamps its own: rank from the phase, step_id from the counter.
    journal = stamp_history_ordering(journal)
    flows = finalize_flows(journal)
    # The load boundary: the historical journal is checked once here, so bad
    # input data fails now instead of surfacing later as a run-end violation.
    violations = check_journal_schema(flows)
    if violations:
        raise ValueError(
            "historical flow journal breaks the journal schema:\n" + "\n".join(violations)
        )
    return flows


def get_trip_speed_km_per_period(
    trips_df: pd.DataFrame, facilities_geo_df: pd.DataFrame, period_len: pd.Timedelta
) -> float:
    """Mean riding speed over the historical trips, in kilometres per period (distance-weighted)."""
    coords = facilities_geo_df.set_index("facility_id")
    distance_km = haversine_km(
        trips_df["start_station_id"].map(coords["lat"]),
        trips_df["start_station_id"].map(coords["lng"]),
        trips_df["end_station_id"].map(coords["lat"]),
        trips_df["end_station_id"].map(coords["lng"]),
    )
    ride_periods = (trips_df["ended_at"] - trips_df["started_at"]) / period_len
    valid = distance_km.notna() & (distance_km > 0) & (ride_periods > 0)
    if not valid.any():
        raise ValueError("no trip with distinct stations and positive ride time to compute speed")
    return float(distance_km[valid].sum() / ride_periods[valid].sum())


# ---------------------------------------------------------------------------
# Entity definitions
# ---------------------------------------------------------------------------
def get_facilities_df(stations_df: pd.DataFrame, depots_df: pd.DataFrame) -> pd.DataFrame:
    """Combine stations and depots into one facility table with categories."""
    return pd.concat(
        [
            stations_df[["station_id"]]
            .rename(columns={"station_id": "facility_id"})
            .assign(facility_category="station"),
            depots_df[["depot_id"]]
            .rename(columns={"depot_id": "facility_id"})
            .assign(facility_category="depot"),
        ],
        ignore_index=True,
    )


def get_resources_df(trucks_df: pd.DataFrame) -> pd.DataFrame:
    """Build the resource table from trucks: id, category, home facility."""
    out = trucks_df[["truck_id", "home_depot_id"]].rename(
        columns={"truck_id": "resource_id", "home_depot_id": "home_facility_id"}
    )
    out.insert(1, "resource_category", "truck")
    return out


def get_commodities_categories_df() -> pd.DataFrame:
    """Return the two bike commodity categories."""
    return pd.DataFrame(
        {
            "commodity_category": ["classic_bike", "electric_bike"],
        }
    )


def get_facilities_geo_df(stations_df: pd.DataFrame, depots_df: pd.DataFrame) -> pd.DataFrame:
    """Geographical attributes: facility_id, lat, lng."""
    return pd.concat(
        [
            stations_df[["station_id", "lat", "lng"]].rename(columns={"station_id": "facility_id"}),
            depots_df[["depot_id", "lat", "lng"]].rename(columns={"depot_id": "facility_id"}),
        ],
        ignore_index=True,
    )


def get_facilities_capacities_df(
    stations_capacities_df: pd.DataFrame, depot_capacities_df: pd.DataFrame
) -> pd.DataFrame:
    """Capacities: facility_id, capacity."""
    return pd.concat(
        [
            stations_capacities_df.rename(columns={"station_id": "facility_id"}),
            depot_capacities_df.rename(columns={"depot_id": "facility_id"}),
        ],
        ignore_index=True,
    )


def get_resources_capacities_df(trucks_capacities_df: pd.DataFrame) -> pd.DataFrame:
    """resource_id, capacity."""
    return trucks_capacities_df.rename(columns={"truck_id": "resource_id"})


def get_facilities_costs_df(
    stations_costs_df: pd.DataFrame, depot_costs_df: pd.DataFrame
) -> pd.DataFrame:
    """Costs: facility_id, fixed_cost."""
    return pd.concat(
        [
            stations_costs_df.rename(
                columns={"station_id": "facility_id", "fixed_cost_station": "fixed_cost"}
            ),
            depot_costs_df.rename(
                columns={"depot_id": "facility_id", "fixed_cost_depot": "fixed_cost"}
            ),
        ],
        ignore_index=True,
    )


def get_resources_rates_df(trucks_rates_df: pd.DataFrame) -> pd.DataFrame:
    """Rename the truck rate table to the resource schema."""
    return trucks_rates_df.rename(columns={"truck_id": "resource_id"})


def get_commodities_categories_rates_df(bike_rates_df: pd.DataFrame) -> pd.DataFrame:
    """Rename the bike rate table to the commodity schema."""
    return bike_rates_df.rename(columns={"rideable_type": "commodity_category"})


def apply_truck_fleet(
    resolved: ResolvedModelData,
    truck_homes: list[str],
    truck_capacity_bikes: int,
    truck_rate: float,
) -> ResolvedModelData:
    """Return a shallow copy of ``resolved`` with a new truck fleet (one entry per truck)."""
    if not truck_homes:
        raise ValueError("truck_homes is empty: the fleet needs at least one truck")
    facilities = resolved.facilities_df
    depots = set(facilities.loc[facilities["facility_category"] == "depot", "facility_id"])
    unknown = sorted(set(truck_homes) - depots)
    if unknown:
        raise ValueError(f"truck homes are not depot facilities: {unknown}")

    trucks_df = pd.DataFrame(
        {
            "truck_id": [f"truck_{i + 1}" for i in range(len(truck_homes))],
            "home_depot_id": list(truck_homes),
        }
    )
    out = copy.copy(resolved)
    out.resources_df = get_resources_df(trucks_df)
    out.resources_capacities_df = get_resources_capacities_df(
        get_trucks_capacities_df(truck_capacity_bikes, trucks_df)
    )
    out.resources_rates_df = get_resources_rates_df(get_trucks_rates_df(truck_rate, trucks_df))
    return out


# ---------------------------------------------------------------------------
# The domain's entry point: raw tables in, resolved model data out
# ---------------------------------------------------------------------------
def build_resolved(
    raw: RawModelData,
    period_len: pd.Timedelta = DEFAULT_PERIOD_LEN,
    routing_mode: RoutingMode = "haversine",
    osrm_url: str = DEFAULT_OSRM_URL,
) -> ResolvedModelData:
    """Build the resolved model data of one Citi Bike scenario from its raw tables.

    Everything domain-specific happens here: raw column names become the
    framework's, the trips become the historical flow journal, and the trip
    timestamps give the mean riding speed. ``ResolvedModelData`` derives the
    marginals and the start inventory from those.
    """
    t0 = raw.trips_df["started_at"].min().floor("h")
    facilities_geo_df = get_facilities_geo_df(raw.stations_df, raw.depots_df)
    return ResolvedModelData(
        facilities_df=get_facilities_df(raw.stations_df, raw.depots_df),
        resources_df=get_resources_df(raw.trucks_df),
        commodities_categories_df=get_commodities_categories_df(),
        facilities_geo_df=facilities_geo_df,
        facilities_capacities_df=get_facilities_capacities_df(
            raw.stations_capacities_df, raw.depot_capacities_df
        ),
        resources_capacities_df=get_resources_capacities_df(raw.trucks_capacities_df),
        facilities_costs_df=get_facilities_costs_df(raw.stations_costs_df, raw.depot_costs_df),
        resources_rates_df=get_resources_rates_df(raw.trucks_rates_df),
        commodities_categories_rates_df=get_commodities_categories_rates_df(raw.bike_rates_df),
        periods_df=get_periods_df(raw.trips_df, t0, period_len),
        historical_flows_df=get_historical_flows_df(raw.trips_df, t0, period_len),
        t0=t0,
        period_len=period_len,
        trip_speed_km_per_period=get_trip_speed_km_per_period(
            raw.trips_df, facilities_geo_df, period_len
        ),
        routing_mode=routing_mode,
        osrm_url=osrm_url,
        trips_path=raw.trips_path,
    )
