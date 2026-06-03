FLOW_EVENT_COLUMNS = [
    "event_id", "period_id", "flow_id", "flow_type", "event_type", "commodity_category",
    "source_id", "planned_target_id", "realized_target_id",
    "start_period", "planned_end_period", "realized_end_period",
    "resource_id", "quantity", "reason",
]

FLOW_EVENT_DTYPES = {
    "period_id": "Int64",
    "flow_id": "string",
    "flow_type": "string",
    "event_type": "string",
    "commodity_category": "string",
    "source_id": "string",
    "planned_target_id": "string",
    "realized_target_id": "string",
    "start_period": "Int64",
    "planned_end_period": "Int64",
    "realized_end_period": "Int64",
    "resource_id": "string",
    "quantity": "Int64",
    "reason": "string",
}



@dataclasses.dataclass
class ResolvedModelData:
    """Everything the engine reads, built once per scenario from the graph data."""

    periods: pd.DataFrame                 # period_id, start_timestamp, end_timestamp
    inventory_initial: pd.DataFrame       # facility_id, commodity_category, quantity
    potential_trips: pd.DataFrame         # the demand: concrete desired trips
    facilities_capacities: pd.DataFrame   # facility_id, capacity   (redirect only)
    facilities_geo: pd.DataFrame          # facility_id, lat, lng   (redirect only)

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
# Marginals derived from the flow log
# ---------------------------------------------------------------------------
def get_historical_departures_df(historical_flows_df: pd.DataFrame) -> pd.DataFrame:
    return (
        historical_flows_df.query("event_type == 'departed'")
        .groupby(["period_id", "source_id", "commodity_category"], as_index=False)["quantity"].sum()
        .rename(columns={"source_id": "facility_id", "commodity_category": "commodity_category"})
    )


def get_historical_arrivals_df(historical_flows_df: pd.DataFrame) -> pd.DataFrame:
    return (
        historical_flows_df.query("event_type == 'arrived'")
        .groupby(["period_id", "realized_target_id", "commodity_category"], as_index=False)["quantity"].sum()
        .rename(columns={"realized_target_id": "facility_id", "commodity_category": "commodity_category"})
    )


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

# Entities
facilities_df = get_facilities_df(stations_df, depots_df)
resources_df = get_resources_df(trucks_df)
commodities_categories_df = get_commodities_categories_df()

facilities_geo_df = get_facilities_geo_df(stations_df, depots_df)
facilities_capacities_df = get_facilities_capacities_df(stations_capacities_df, depot_capacities_df)
resources_capacities_df = get_resources_capacities_df(trucks_capacities_df)

facilities_costs_df = get_facilities_costs_df(stations_costs_df, depot_costs_df)
resources_rates_df = get_resources_rates_df(trucks_rates_df)
commodities_categories_rates_df = get_commodities_categories_rates_df(bike_rates_df)


# ---------------------------------------------------------------------------
# Возможно, тут надо разделить. Так как то что ДО - это общая часть. А то что после. - это уже про конкретные flow.
# А может и не нужно...
# Скорее всего это может быть оправдано. Напримери у меня есть исторический сценарий.
# А могут быть разные эксперименты. И каждый из них - это отдельный экземпляр класса.
# И потом можно будет легче сделать логику в рамках которой будут происходить сравнения экспериментов.
# ---------------------------------------------------------------------------

# Period grid
# Это надо потом будет в конфиг как то грамотно записать. И чтобы автоматически пересчитывалось.
PERIOD_LEN = pd.Timedelta(hours=1)
t0 = trips_df["started_at"].min().floor("h")
periods_df = get_periods_df(trips_df, t0, PERIOD_LEN)

initial_inventory_df = get_initial_inventory_df(gbfs_raw_df, stations_df)

# Historical observations
## General:
historical_flows_df = get_historical_flows_df(trips_df, t0, PERIOD_LEN)
historical_inventory_df = None
historical_resources_df = None
historical_demand_df = get_historical_arrivals_df(historical_flows_df)
## Additionall
historical_departures_df = get_historical_departures_df(historical_flows_df)
historical_arrivals_df = historical_demand_df.copy()
historical_od_matrix_df = None

# Simulator observations declarations
## General:
simulated_flows_df = None
simulated_inventory_df = None
simulated_resources_df = None
simulated_demand_df = None
## Additionall:
simulated_departures_df = None
simulated_arrivals_df = None
simulated_od_matrix_df = None