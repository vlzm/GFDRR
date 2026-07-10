"""Graph (resolved) model data.

The period grid, the historical flow log, the graph entities/attributes, and
the replay demand the engine consumes.

``ResolvedModelData`` is built once per scenario from a :class:`RawModelData`
and exposes the graph tables. The :class:`~engine.Environment` and its phases
read a narrow subset of them: ``periods_df``, ``initial_inventory_df``,
``historical_demand_df``, ``historical_od_matrix_df``,
``facilities_capacities_df``, ``facilities_geo_df`` and ``routes``.
"""

import copy

import pandas as pd
import pandera.pandas as pa

from gbp.loaders.dataloader_raw import (
    RawModelData,
    get_trucks_capacities_df,
    get_trucks_rates_df,
)
from gbp.model import (
    arrived_events,
    departed_events,
    finalize_flows,
    flows_to_arrivals,
    flows_to_departures,
    flows_to_od_matrix,
    get_inventory_df,
    haversine_km,
    inventory_at_moments,
    phase_rank_by_timing,
)
from gbp.model.journal_schema import check_journal_schema, schema_violations
from gbp.routing import DEFAULT_OSRM_URL, Routes, RoutingMode

# ---------------------------------------------------------------------------
# Schemas of the tables the engine and its phases read
# ---------------------------------------------------------------------------
# One pandera schema per table the engine reads from ``ResolvedModelData``.
# They are checked once, at the end of ``ResolvedModelData.__init__``; the two
# sized-state tables are checked again in ``run_sized_scenario`` right after
# sizing replaces them. The id and category columns keep their dtype
# unchecked on purpose (the sources mix pandas ``string`` and plain ``object``
# columns); the contract is the column set, non-null ids, non-negative
# quantities, and unique keys.

#: The period grid: one row per ``period_id`` with its wall-clock bounds.
PERIODS_SCHEMA = pa.DataFrameSchema(
    columns={
        "period_id": pa.Column(nullable=False, unique=True),
        "start_timestamp": pa.Column(nullable=False),
        "end_timestamp": pa.Column(nullable=False),
    },
    strict=False,
    name="periods",
)

#: Starting inventory: one row per ``(facility_id, commodity_category)``.
INITIAL_INVENTORY_SCHEMA = pa.DataFrameSchema(
    columns={
        "facility_id": pa.Column(nullable=False),
        "commodity_category": pa.Column(nullable=False),
        "quantity": pa.Column(checks=pa.Check.ge(0), nullable=False),
    },
    unique=["facility_id", "commodity_category"],
    strict=False,
    name="initial_inventory",
)

#: Historical demand marginal: quantity per (period, facility, commodity).
HISTORICAL_DEMAND_SCHEMA = pa.DataFrameSchema(
    columns={
        "period_id": pa.Column(nullable=False),
        "facility_id": pa.Column(nullable=False),
        "commodity_category": pa.Column(nullable=False),
        "quantity": pa.Column(checks=pa.Check.ge(0), nullable=False),
    },
    strict=False,
    name="historical_demand",
)

#: The OD matrix: target probability and mean duration per source pair.
HISTORICAL_OD_MATRIX_SCHEMA = pa.DataFrameSchema(
    columns={
        "source_id": pa.Column(nullable=False),
        "planned_target_id": pa.Column(nullable=False),
        "period_id": pa.Column(nullable=False),
        "commodity_category": pa.Column(nullable=False),
        "count": pa.Column(checks=pa.Check.ge(1), nullable=False),
        "duration": pa.Column(checks=pa.Check.ge(0), nullable=False),
        "probability": pa.Column(checks=[pa.Check.gt(0), pa.Check.le(1)], nullable=False),
    },
    strict=False,
    name="historical_od_matrix",
)

#: Dock capacities: one row per facility.
FACILITIES_CAPACITIES_SCHEMA = pa.DataFrameSchema(
    columns={
        "facility_id": pa.Column(nullable=False, unique=True),
        "capacity": pa.Column(checks=pa.Check.ge(0), nullable=False),
    },
    strict=False,
    name="facilities_capacities",
)

#: Facility geography: one coordinate pair per facility.
FACILITIES_GEO_SCHEMA = pa.DataFrameSchema(
    columns={
        "facility_id": pa.Column(nullable=False, unique=True),
        "lat": pa.Column(nullable=False),
        "lng": pa.Column(nullable=False),
    },
    strict=False,
    name="facilities_geo",
)

#: The engine-facing tables of ``ResolvedModelData``, with the schema of each
#: (the same list as the class docstring, ``routes`` excluded -- it is an
#: object, not a table).
ENGINE_TABLE_SCHEMAS = {
    "periods_df": PERIODS_SCHEMA,
    "initial_inventory_df": INITIAL_INVENTORY_SCHEMA,
    "historical_demand_df": HISTORICAL_DEMAND_SCHEMA,
    "historical_od_matrix_df": HISTORICAL_OD_MATRIX_SCHEMA,
    "facilities_capacities_df": FACILITIES_CAPACITIES_SCHEMA,
    "facilities_geo_df": FACILITIES_GEO_SCHEMA,
}

# ---------------------------------------------------------------------------
# Period grid (the simulation clock)
# ---------------------------------------------------------------------------
#: Default length of a single simulation period.
DEFAULT_PERIOD_LEN = pd.Timedelta(hours=1)


def to_period_id(ts: pd.Series, t0: pd.Timestamp, period_len: pd.Timedelta) -> pd.Series:
    """Map timestamps to zero-based period ids relative to ``t0``."""
    return ((ts - t0) // period_len).astype("int64")


def hour_of_week(ts: pd.Series) -> pd.Series:
    """Map timestamps to the hour of the week: ``weekday * 24 + hour``, 0..167.

    0 is Monday 00:00. Two timestamps in different weeks share a value when
    they fall on the same weekday and hour — the key the forecast path uses to
    carry weekly patterns (demand averages, OD shares) onto future periods.
    """
    return (ts.dt.dayofweek * 24 + ts.dt.hour).astype("int64")


def get_periods_df(
    trips_df: pd.DataFrame, t0: pd.Timestamp, period_len: pd.Timedelta
) -> pd.DataFrame:
    """Build the period grid covering every trip, with start/end timestamps."""
    n_periods = int(to_period_id(trips_df["ended_at"], t0, period_len).max()) + 1
    periods_df = pd.DataFrame({"period_id": range(n_periods)})
    periods_df["start_timestamp"] = t0 + periods_df["period_id"] * period_len
    periods_df["end_timestamp"] = periods_df["start_timestamp"] + period_len
    return periods_df


def get_forecast_periods_df(
    t0: pd.Timestamp, number_of_periods: int, period_len: pd.Timedelta
) -> pd.DataFrame:
    """Build the period grid of a forecast horizon: ``number_of_periods`` from ``t0``.

    The same shape as :func:`get_periods_df`, but the length comes from the
    forecast horizon instead of the last trip. Period ids restart at 0: a
    forecast run is its own scenario with its own clock, and the run machinery
    (the demand filter per period, the invariant checks, the panel) all count
    periods from 0.
    """
    periods_df = pd.DataFrame({"period_id": range(number_of_periods)})
    periods_df["start_timestamp"] = t0 + periods_df["period_id"] * period_len
    periods_df["end_timestamp"] = periods_df["start_timestamp"] + period_len
    return periods_df


# ---------------------------------------------------------------------------
# Flow event log
# ---------------------------------------------------------------------------
def get_historical_flows_df(
    trips_df: pd.DataFrame, t0: pd.Timestamp, period_len: pd.Timedelta
) -> pd.DataFrame:
    """Expand each historical trip into a realized-flow event log.

    Ground-truth history contains only flows that actually happened, so each
    completed trip is one flow that emits two events in order: ``departed``
    (move 0, event 0) at the start period and ``arrived`` (move 0, event 1) at
    the end period. History never redirects, so every historical flow stays on a
    single arc (``move_id == 0``). The outcomes that exist only under simulation
    (``lost``, ``redirected``) are intentionally absent here — this is a
    representation of input data, not the simulator's own journal. Each field is
    filled only by the event that determines it (e.g. ``realized_end_period`` is
    null until ``arrived``); the full picture of a flow is recovered by stitching
    its rows on ``flow_id``.

    ``flow_id`` is namespaced with a ``hist_`` prefix so it cannot collide with
    flows the simulator generates and appends to the same journal.

    The rows are built with the shared :func:`~gbp.model.flows.departed_events` /
    :func:`~gbp.model.flows.arrived_events` builders and ordered by
    :func:`~gbp.model.flows.finalize_flows` -- the same primitives the simulator uses --
    so a base replay's finalized journal is identical to this log by
    construction.

    Parameters
    ----------
    trips_df : pandas.DataFrame
        Trips with ``started_at``, ``ended_at``, ``start_station_id``,
        ``end_station_id`` and ``rideable_type``. The row index seeds ``flow_id``.
    t0 : pandas.Timestamp
        Start of the period grid.
    period_len : pandas.Timedelta
        Length of a single period.

    Returns
    -------
    pandas.DataFrame
        Event log with columns :data:`FLOW_EVENT_COLUMNS`, sorted by
        ``period_id`` then ``flow_id`` then ``event_id`` (the per-trip
        ``move_id`` / ``event_id`` are set by the builders).
    """
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
    # The loader has no phases, so it stamps phase_rank with the timing rule
    # (departed -> period-own; an arrival -> dock-same or dock-previous by whether
    # it closes in its own period). The simulator stamps its phase's rank instead.
    journal["phase_rank"] = phase_rank_by_timing(journal)
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
    """Mean riding speed over the historical trips, in kilometres per period.

    Total great-circle distance divided by total ride time, so long trips weigh
    more than short ones. Speed must come from the raw ``started_at`` /
    ``ended_at`` timestamps: the OD matrix stores durations rounded to whole
    periods, and most trips are shorter than one period, so a speed computed
    from the OD matrix would divide by near-zero times.

    Trips that start and end at the same station, take no time, or miss a
    coordinate carry no speed information and are skipped.

    :class:`gbp.routing.Routes` uses this value to turn a straight-line
    distance into a travel time — in the ``haversine`` routing mode for every
    pair, in the ``osrm`` mode only for pairs the server cannot route.

    Parameters
    ----------
    trips_df : pandas.DataFrame
        Trips with ``started_at``, ``ended_at``, ``start_station_id``,
        ``end_station_id``.
    facilities_geo_df : pandas.DataFrame
        Facility geography: ``facility_id``, ``lat``, ``lng``.
    period_len : pandas.Timedelta
        Length of a single period.

    Returns
    -------
    float
        Kilometres a bike rides in one period, on average.
    """
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
    """Build the resource table from trucks: id, category, home facility.

    ``home_facility_id`` is the depot the truck starts and ends its
    rebalancing route at (Notations.md §14).
    """
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
    resolved: "ResolvedModelData",
    truck_homes: list[str],
    truck_capacity_bikes: int,
    truck_rate: float,
) -> "ResolvedModelData":
    """Return a shallow copy of ``resolved`` with a new truck fleet.

    ``truck_homes`` lists the home depot of each truck, one entry per truck:
    ``["depot_1", "depot_1", "depot_3"]`` is a fleet of three trucks, two
    based at ``depot_1`` and one at ``depot_3``. The fleet is a run
    parameter: the heavy graph tables are untouched, only the three resource
    tables (``resources_df``, ``resources_capacities_df``,
    ``resources_rates_df``) are rebuilt.

    Parameters
    ----------
    resolved : ResolvedModelData
        The resolved scenario data. Not modified.
    truck_homes : list of str
        Home depot per truck; every entry must be a depot facility.
    truck_capacity_bikes : int
        Bikes one truck can carry.
    truck_rate : float
        Price per hour of truck use, in dollars.

    Returns
    -------
    ResolvedModelData
        A shallow copy carrying the new fleet.
    """
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
# Forecast path: run the scenario on a forecast demand table (Notations.md §17)
# ---------------------------------------------------------------------------
def map_od_matrix_by_hour_of_week(
    od_matrix_df: pd.DataFrame,
    periods_df: pd.DataFrame,
    forecast_periods_df: pd.DataFrame,
) -> pd.DataFrame:
    """Carry the historical OD matrix onto forecast periods by hour of week.

    Forecast periods have no history, so they have no OD matrix of their own.
    This builds one from the historical matrix: pool the historical rows that
    share an hour of week (Monday 08:00 across all weeks is one pool), then
    give every forecast period the pool of its own hour of week.

    Within one pool, per ``(source, target, commodity)``: ``count`` is the sum
    of the historical counts, ``duration`` is the count-weighted mean of the
    historical mean durations (rounded to whole periods), and ``probability``
    is recomputed as the pair's share of the pool's total per
    ``(source, commodity)`` — so the shares sum to 1 again.

    Parameters
    ----------
    od_matrix_df : pandas.DataFrame
        The historical OD matrix (``HISTORICAL_OD_MATRIX_SCHEMA``).
    periods_df : pandas.DataFrame
        The historical period grid; gives each OD row its hour of week.
    forecast_periods_df : pandas.DataFrame
        The forecast period grid (:func:`get_forecast_periods_df`).

    Returns
    -------
    pandas.DataFrame
        An OD matrix in the same schema whose ``period_id`` values are the
        forecast periods.
    """
    period_hours = periods_df[["period_id"]].assign(
        hour_of_week=hour_of_week(periods_df["start_timestamp"])
    )
    od = od_matrix_df.merge(period_hours, on="period_id", how="left")
    if od["hour_of_week"].isna().any():
        missing = od.loc[od["hour_of_week"].isna(), "period_id"].unique()[:5].tolist()
        raise ValueError(f"OD matrix has periods outside the period grid: {missing}")

    od["duration_x_count"] = od["duration"] * od["count"]
    pooled = od.groupby(
        ["source_id", "planned_target_id", "hour_of_week", "commodity_category"], as_index=False
    ).agg(count=("count", "sum"), duration_x_count=("duration_x_count", "sum"))
    pooled["duration"] = (pooled["duration_x_count"] / pooled["count"]).round().astype("Int64")
    totals = pooled.groupby(["source_id", "hour_of_week", "commodity_category"])["count"].transform(
        "sum"
    )
    pooled["probability"] = pooled["count"] / totals

    forecast_hours = forecast_periods_df[["period_id"]].assign(
        hour_of_week=hour_of_week(forecast_periods_df["start_timestamp"])
    )
    out = pooled.merge(forecast_hours, on="hour_of_week", how="inner")
    return out[
        [
            "source_id",
            "planned_target_id",
            "period_id",
            "commodity_category",
            "count",
            "duration",
            "probability",
        ]
    ].reset_index(drop=True)


def apply_forecast_demand(
    resolved: "ResolvedModelData",
    forecast_demand_df: pd.DataFrame,
    forecast_periods_df: pd.DataFrame,
) -> "ResolvedModelData":
    """Return a shallow copy of ``resolved`` that runs on a forecast demand table.

    The copy carries the forecast period grid, the forecast demand table, and
    a historical OD matrix mapped onto the forecast periods by hour of week
    (:func:`map_od_matrix_by_hour_of_week`). Everything else — facilities,
    capacities, routes, the historical observations — is shared as-is. The
    forecast table sits in the ``historical_demand_df`` slot because that is
    the one demand slot the engine reads; the run's ``meta.json`` records that
    the demand came from a forecast (``demand_source``, ``forecast_name``).

    Like the loader itself, this is a load boundary: the three replaced tables
    are schema-checked here, plus two cross-table checks a schema cannot
    express — every demand facility must exist in the facility table, and
    every demanded ``(facility, commodity, period)`` must have OD rows, or the
    engine would silently drop those departures and break the demand-split
    invariant (I1).

    Parameters
    ----------
    resolved : ResolvedModelData
        The resolved scenario data. Not modified.
    forecast_demand_df : pandas.DataFrame
        A forecast demand table (Notations.md §17): whole-bike quantities in
        ``HISTORICAL_DEMAND_SCHEMA`` shape, on the forecast period grid.
    forecast_periods_df : pandas.DataFrame
        The forecast period grid (:func:`get_forecast_periods_df`). Its period
        length must equal the grid of ``resolved`` — OD durations are counted
        in periods, so a different length would re-time every trip.

    Returns
    -------
    ResolvedModelData
        A shallow copy carrying the forecast demand, grid, and OD matrix.
    """
    violations = [
        *schema_violations(PERIODS_SCHEMA, forecast_periods_df),
        *schema_violations(HISTORICAL_DEMAND_SCHEMA, forecast_demand_df),
    ]
    if violations:
        raise ValueError("forecast tables break their schemas:\n" + "\n".join(violations))

    period_len = (
        forecast_periods_df["end_timestamp"].iloc[0]
        - (forecast_periods_df["start_timestamp"].iloc[0])
    )
    if period_len != resolved.period_len:
        raise ValueError(
            f"forecast period length {period_len} != scenario period length {resolved.period_len}"
        )

    known = set(resolved.facilities_df["facility_id"])
    unknown = sorted(set(forecast_demand_df["facility_id"]) - known)
    if unknown:
        raise ValueError(f"forecast demand names unknown facilities: {unknown[:5]}")

    od_matrix_df = map_od_matrix_by_hour_of_week(
        resolved.historical_od_matrix_df, resolved.periods_df, forecast_periods_df
    )

    demanded = forecast_demand_df[forecast_demand_df["quantity"] > 0]
    covered = od_matrix_df[["source_id", "period_id", "commodity_category"]].drop_duplicates()
    coverage = demanded.merge(
        covered.rename(columns={"source_id": "facility_id"}),
        on=["facility_id", "period_id", "commodity_category"],
        how="left",
        indicator=True,
    )
    uncovered = coverage[coverage["_merge"] == "left_only"]
    if not uncovered.empty:
        sample = uncovered[["period_id", "facility_id", "commodity_category"]].head(5)
        raise ValueError(
            f"{len(uncovered)} forecast demand rows have no OD rows for their "
            f"(facility, commodity, period); first rows:\n{sample.to_string(index=False)}"
        )

    out = copy.copy(resolved)
    out.periods_df = forecast_periods_df
    out.t0 = forecast_periods_df["start_timestamp"].iloc[0]
    out.historical_demand_df = forecast_demand_df
    out.historical_od_matrix_df = od_matrix_df
    return out


# ---------------------------------------------------------------------------
# Resource observations
# ---------------------------------------------------------------------------
RESOURCE_OBS_COLUMNS = ["period_id", "resource_id", "facility_id", "load"]


def empty_resources_obs_df() -> pd.DataFrame:
    """Empty resource-observation table (trucks are idle in the replay)."""
    return pd.DataFrame(
        {
            "period_id": pd.Series(dtype="Int64"),
            "resource_id": pd.Series(dtype="string"),
            "facility_id": pd.Series(dtype="string"),
            "load": pd.Series(dtype="Int64"),
        }
    )


# ---------------------------------------------------------------------------
# Self-consistent initial state for a clean replay (no stockout / dock-full)
# ---------------------------------------------------------------------------
def get_replay_initial_inventory_df(
    historical_flows_df: pd.DataFrame,
    facilities_df: pd.DataFrame,
    commodities_categories_df: pd.DataFrame,
) -> pd.DataFrame:
    """Smallest initial inventory that lets the replay run with no stockout.

    A stockout is checked *inside* a period, during the departures phase, before
    that period's own same-period arrivals are docked (a same-period arrival is a
    trip that both starts and ends within the one period). The binding low point
    of inventory is therefore per inventory step (Notations.md "step"), not per
    period: the end-of-period value already counts those late same-period
    arrivals, so it overstates what is on hand at the moment of departure. Sizing
    the start inventory against the per-period low point leaves real stockouts.

    Starting from zero inventory, :func:`inventory_at_moments` gives the inventory
    after every step; its per-``(facility, commodity)`` minimum is the deepest the
    trajectory ever goes. Holding that much inventory at the start lifts the whole
    trajectory so its floor is exactly zero, and every historical departure finds
    a bike.

    Returns
    -------
    pandas.DataFrame
        One row per ``(station, commodity)`` with ``facility_id``,
        ``commodity_category`` and ``quantity``.
    """
    stations = facilities_df.loc[facilities_df["facility_category"] == "station", ["facility_id"]]
    grid = stations.merge(commodities_categories_df[["commodity_category"]], how="cross")

    moments = inventory_at_moments(historical_flows_df, grid.assign(quantity=0))
    low = moments.groupby(["facility_id", "commodity_category"], as_index=False)[
        "inventory_after"
    ].min()
    low["quantity"] = (-low["inventory_after"]).clip(lower=0).astype("int64")

    out = grid.merge(
        low[["facility_id", "commodity_category", "quantity"]],
        on=["facility_id", "commodity_category"],
        how="left",
    )
    out["quantity"] = out["quantity"].fillna(0).astype("int64")
    return out[["facility_id", "commodity_category", "quantity"]]


def get_replay_capacities_df(
    historical_flows_df: pd.DataFrame,
    initial_inventory_df: pd.DataFrame,
    facilities_capacities_df: pd.DataFrame,
    min_capacity: int = 10,
) -> pd.DataFrame:
    """Smallest dock capacities that let the replay run with no dock-full/redirect.

    The mirror of :func:`get_replay_initial_inventory_df`. Dock capacity is per
    facility, shared across commodities, and a dock-full (then a redirect) happens
    when incoming bikes would push the facility's total occupancy above its
    capacity. With the initial inventory fixed, :func:`inventory_at_moments` gives
    the occupancy after every step; the per-facility peak of the total across
    commodities is the most docks ever needed at once. A capacity equal to that
    peak holds every arrival, so no flow is ever redirected.

    The peak must include the *initial* occupancy (the moment before the first
    step), not only the after-step values. A station whose inventory only drains
    early on has its all-time high at the start; taking the peak over after-step
    values alone would set its capacity below the bikes it already holds, so its
    free docks would read as zero and every arrival there would redirect.

    Parameters
    ----------
    historical_flows_df : pandas.DataFrame
        The flow log to size against (historical, or a sizing run's journal).
    initial_inventory_df : pandas.DataFrame
        The start inventory to size against (use the output of
        :func:`get_replay_initial_inventory_df`).
    facilities_capacities_df : pandas.DataFrame
        The capacity table whose ``facility_id`` set defines the output rows.
    min_capacity : int, optional
        A floor applied to every facility, so facilities with no replay traffic
        (e.g. depots) keep a usable capacity. Defaults to 10.

    Returns
    -------
    pandas.DataFrame
        ``facility_id`` and ``capacity`` set to each facility's required peak,
        floored at ``min_capacity``.
    """
    moments = inventory_at_moments(historical_flows_df, initial_inventory_df)
    facility_total = moments.groupby(["step_id", "facility_id"], as_index=False)[
        "inventory_after"
    ].sum()
    step_peak = facility_total.groupby("facility_id")["inventory_after"].max()
    # The initial occupancy is the moment before the first step; a facility whose
    # inventory only drains has its all-time high here, not at any after-step value.
    initial_total = initial_inventory_df.groupby("facility_id")["quantity"].sum()
    idx = step_peak.index.union(initial_total.index)
    peak = (
        pd.concat(
            [step_peak.reindex(idx, fill_value=0), initial_total.reindex(idx, fill_value=0)],
            axis=1,
        )
        .max(axis=1)
        .rename("peak_occupancy")
        .rename_axis("facility_id")
        .reset_index()
    )

    out = facilities_capacities_df[["facility_id"]].merge(peak, on="facility_id", how="left")
    out["capacity"] = out["peak_occupancy"].fillna(0).clip(lower=min_capacity).astype("int64")
    return out[["facility_id", "capacity"]]


# ---------------------------------------------------------------------------
# Saturated (artificial) initial state for the base replay
# ---------------------------------------------------------------------------
def get_saturated_inventory_df(
    facilities_df: pd.DataFrame,
    commodities_categories_df: pd.DataFrame,
    quantity: int = 1_000_000,
) -> pd.DataFrame:
    """Build artificial initial inventory holding ``quantity`` bikes per station.

    Every station holds ``quantity`` bikes of each commodity.
    Used by the base scenario instead of a snapshot of today's real station
    inventory. Such a snapshot is a *current* observation, unrelated to the
    historical start state, so limiting
    demand against it starves the replay (most departures lose to a stockout that
    never happened historically). With inventory far above any period's demand the
    limit never takes effect, every historical departure departs, and the run reproduces
    the historical departures exactly even though demand is still limited and trips
    are still formed from the OD matrix.

    Returns
    -------
    pandas.DataFrame
        ``facility_id``, ``commodity_category``, ``quantity`` for every
        ``(station, commodity)``.
    """
    stations = facilities_df.loc[facilities_df["facility_category"] == "station", ["facility_id"]]
    inv = stations.merge(commodities_categories_df[["commodity_category"]], how="cross")
    inv["quantity"] = quantity
    return inv.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Resolved model data container
# ---------------------------------------------------------------------------
class ResolvedModelData:
    """Graph data for one scenario, built from a :class:`RawModelData`.

    Exposes the rich graph tables (entities, attributes, historical
    observations). The engine and its phases read directly: ``periods_df``,
    ``initial_inventory_df``, ``historical_demand_df``,
    ``historical_od_matrix_df``, ``facilities_capacities_df``,
    ``facilities_geo_df`` and ``routes``.

    ``initial_inventory_df`` and ``facilities_capacities_df`` are built for the
    base replay: the smallest state that runs the historical demand with no
    stockout and no dock-full. To run a *scaled* demand, replace both with the
    output of :func:`gbp.consumers.simulator.size_state_for_demand`, which sizes
    them against the scaled scenario's own journal.

    Parameters
    ----------
    raw : RawModelData
        The loaded raw entity tables.
    period_len : pandas.Timedelta, optional
        Length of a single simulation period. Defaults to one hour.
    routing_mode : {"haversine", "osrm"}, optional
        How ``routes`` measures distance and travel time between facilities
        (see :mod:`gbp.routing`). Defaults to ``"haversine"``. The ``"osrm"``
        mode needs a running OSRM server and fetches the full
        facility-to-facility table here, once.
    osrm_url : str, optional
        Base URL of the OSRM server. Only read when ``routing_mode="osrm"``.
    """

    def __init__(
        self,
        raw: RawModelData,
        period_len: pd.Timedelta = DEFAULT_PERIOD_LEN,
        routing_mode: RoutingMode = "haversine",
        osrm_url: str = DEFAULT_OSRM_URL,
    ) -> None:
        # Where the raw data came from, so a saved run can record it in the
        # ``inputs`` field of ``meta.json``.
        self.trips_path = raw.trips_path

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
        self.facilities_costs_df = get_facilities_costs_df(
            raw.stations_costs_df, raw.depot_costs_df
        )
        self.resources_rates_df = get_resources_rates_df(raw.trucks_rates_df)
        self.commodities_categories_rates_df = get_commodities_categories_rates_df(
            raw.bike_rates_df
        )

        # Time grid
        self.period_len = period_len
        self.t0 = raw.trips_df["started_at"].min().floor("h")
        self.periods_df = get_periods_df(raw.trips_df, self.t0, period_len)

        # Historical observations: the marginals of the flow log, derived with
        # the same read-model functions as the simulated set below.
        self.historical_flows_df = get_historical_flows_df(raw.trips_df, self.t0, period_len)
        self.historical_resources_df = empty_resources_obs_df()

        historical_departures_df = flows_to_departures(self.historical_flows_df)

        # Size the start inventory against the per-step low point of the inventory
        # trajectory, so the replay never hits a stockout (see the function's
        # docstring for why the per-period low point is not enough).
        self.initial_inventory_df = get_replay_initial_inventory_df(
            self.historical_flows_df, self.facilities_df, self.commodities_categories_df
        )
        self.historical_inventory_df = get_inventory_df(
            self.historical_flows_df, self.initial_inventory_df
        )

        self.historical_demand_df = historical_departures_df
        self.historical_departures_df = historical_departures_df
        self.historical_arrivals_df = flows_to_arrivals(self.historical_flows_df)
        self.historical_od_matrix_df = flows_to_od_matrix(self.historical_flows_df)

        # Mean riding speed from the raw timestamps; Routes turns straight-line
        # distances into travel times with it (see get_trip_speed_km_per_period).
        self.trip_speed_km_per_period = get_trip_speed_km_per_period(
            raw.trips_df, self.facilities_geo_df, period_len
        )

        # The one distance / travel-time answerer for facility pairs. In osrm
        # mode this fetches the full facility-to-facility table now, once.
        self.routing_mode = routing_mode
        self.routes = Routes(
            self.facilities_geo_df,
            routing_mode,
            trip_speed_km_per_period=self.trip_speed_km_per_period,
            period_len=period_len,
            osrm_url=osrm_url,
        )

        # Simulated observations -- filled by attach_simulation after a run
        self.simulated_flows_df: pd.DataFrame | None = None
        self.simulated_resources_df: pd.DataFrame | None = None
        self.simulated_inventory_df: pd.DataFrame | None = None
        self.simulated_demand_df: pd.DataFrame | None = None
        self.simulated_departures_df: pd.DataFrame | None = None
        self.simulated_arrivals_df: pd.DataFrame | None = None
        self.simulated_od_matrix_df: pd.DataFrame | None = None

        # Consistency check: the start-of-period inventory at period 0 is, by
        # construction, the initial inventory. Verify the two agree per commodity
        # category, so a mismatch in how either is built is caught early.
        init_by_cat = self.initial_inventory_df.groupby("commodity_category")["quantity"].sum()
        sop0_by_cat = (
            self.historical_inventory_df[self.historical_inventory_df["period_id"] == 0]
            .groupby("commodity_category")["quantity_sop"]
            .sum()
        )
        categories = init_by_cat.index.union(sop0_by_cat.index)
        init_by_cat = init_by_cat.reindex(categories, fill_value=0)
        sop0_by_cat = sop0_by_cat.reindex(categories, fill_value=0)
        assert init_by_cat.equals(sop0_by_cat), (
            "initial_inventory does not match historical_inventory quantity_sop at period 0 "
            f"per commodity category:\ninitial_inventory:\n{init_by_cat}\n"
            f"historical_inventory quantity_sop@period 0:\n{sop0_by_cat}"
        )

        # The load boundary: every table the engine reads is checked once
        # here, so a wrong shape fails now instead of as a mid-run pandas
        # error. (The assert above stays: it is a cross-table consistency
        # check, which a per-table schema cannot express.)
        violations = check_engine_tables(self)
        if violations:
            raise ValueError(
                "resolved model data breaks its table schemas:\n" + "\n".join(violations)
            )


def check_engine_tables(resolved: "ResolvedModelData") -> list[str]:
    """Check every engine-facing table of ``resolved`` against its schema.

    Runs each schema of :data:`ENGINE_TABLE_SCHEMAS` with ``lazy=True`` and
    returns all violations as one list (empty = every table is valid).
    """
    violations: list[str] = []
    for attribute, schema in ENGINE_TABLE_SCHEMAS.items():
        violations += schema_violations(schema, getattr(resolved, attribute))
    return violations


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
    resolved.simulated_inventory_df = get_inventory_df(
        resolved.simulated_flows_df, resolved.initial_inventory_df
    )
    simulated_departures_df = flows_to_departures(resolved.simulated_flows_df)
    resolved.simulated_demand_df = simulated_departures_df
    resolved.simulated_departures_df = simulated_departures_df
    resolved.simulated_arrivals_df = flows_to_arrivals(resolved.simulated_flows_df)
    resolved.simulated_od_matrix_df = flows_to_od_matrix(resolved.simulated_flows_df)
