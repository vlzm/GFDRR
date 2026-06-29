"""Graph (resolved) model data.

The period grid, the historical flow log, the graph entities/attributes, and
the replay demand the engine consumes.

``ResolvedModelData`` is built once per scenario from a :class:`RawModelData`
and exposes the graph tables. The :class:`~engine.Environment` reads a narrow
subset of them: ``periods_df``, ``initial_inventory_df``, ``potential_trips_df``,
``facilities_capacities_df`` and ``facilities_geo_df``.
"""

import numpy as np
import pandas as pd

from gbp.loaders.dataloader_raw import RawModelData
from gbp.model import (
    arrived_events,
    departed_events,
    finalize_flows,
    flows_to_arrivals,
    flows_to_departures,
    flows_to_od_matrix,
    get_inventory_df,
)

# ---------------------------------------------------------------------------
# Period grid (the simulation clock)
# ---------------------------------------------------------------------------
#: Default length of a single simulation period.
DEFAULT_PERIOD_LEN = pd.Timedelta(hours=1)


def to_period_id(ts: pd.Series, t0: pd.Timestamp, period_len: pd.Timedelta) -> pd.Series:
    """Map timestamps to zero-based period ids relative to ``t0``."""
    return ((ts - t0) // period_len).astype("int64")


def get_periods_df(
    trips_df: pd.DataFrame, t0: pd.Timestamp, period_len: pd.Timedelta
) -> pd.DataFrame:
    """Build the period grid covering every trip, with start/end timestamps."""
    n_periods = int(to_period_id(trips_df["ended_at"], t0, period_len).max()) + 1
    periods_df = pd.DataFrame({"period_id": range(n_periods)})
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
    return finalize_flows(pd.concat([departed, arrived], ignore_index=True))


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
    """Build the resource table from trucks with their category."""
    return pd.concat(
        [
            trucks_df[["truck_id"]]
            .rename(columns={"truck_id": "resource_id"})
            .assign(resource_category="truck"),
        ],
        ignore_index=True,
    )


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


def get_resources_additional_attributes_df(trucks_df: pd.DataFrame) -> pd.DataFrame:
    """Additional attributes: resource_id, home_facility_id."""
    return trucks_df.rename(columns={"truck_id": "resource_id"}).assign(home_facility_id="depot_1")


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
# Replay demand
# ---------------------------------------------------------------------------
def build_potential_trips(historical_flows_df: pd.DataFrame) -> pd.DataFrame:
    """Replay demand: one concrete desired trip per historical departure.

    Carries the real target and duration of every trip, which is what makes the
    base run reproduce history exactly instead of resampling it. Only move-0
    departures are real user departures; history never redirects, so all its
    departures are already ``move_id == 0``, but the filter is kept defensively.
    """
    cols = [
        "flow_id",
        "source_id",
        "planned_target_id",
        "commodity_category",
        "start_period",
        "planned_end_period",
    ]
    departed = historical_flows_df[
        (historical_flows_df["event_type"] == "departed") & (historical_flows_df["move_id"] == 0)
    ]
    return departed[cols].reset_index(drop=True)


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
    Used by the base scenario instead of the GBFS snapshot. The snapshot is a
    *current* observation, unrelated to the historical start state, so limiting
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
    observations). The engine reads directly: ``periods_df``,
    ``initial_inventory_df``, ``potential_trips_df``,
    ``facilities_capacities_df`` and ``facilities_geo_df``.

    Parameters
    ----------
    raw : RawModelData
        The loaded raw entity tables.
    period_len : pandas.Timedelta, optional
        Length of a single simulation period. Defaults to one hour.
    saturate_stock : bool, optional
        If True, replace the GBFS initial inventory and the dock capacities with
        artificial saturated ones (every station holding far above demand, every
        facility with effectively unbounded docks). This is the base-replay setup:
        demand limiting and overflow redirect stay in the pipeline but never take effect,
        so the run reproduces the historical departures exactly. Defaults to False.
    saturation_quantity : int, optional
        The per-station inventory and per-facility capacity used when
        ``saturate_stock`` is True. Defaults to one million.
    """

    def __init__(
        self,
        raw: RawModelData,
        period_len: pd.Timedelta = DEFAULT_PERIOD_LEN,
        scale_capacity_factor: int = 1,
    ) -> None:
        # Entities
        self.facilities_df = get_facilities_df(raw.stations_df, raw.depots_df)
        self.resources_df = get_resources_df(raw.trucks_df)
        self.commodities_categories_df = get_commodities_categories_df()

        # Attributes
        self.facilities_geo_df = get_facilities_geo_df(raw.stations_df, raw.depots_df)
        self.facilities_capacities_df = get_facilities_capacities_df(
            raw.stations_capacities_df, raw.depot_capacities_df
        )
        self.facilities_capacities_df["capacity"] = (
            self.facilities_capacities_df["capacity"] * scale_capacity_factor
        )
        # if capacity < 10 then capacity = 10
        self.facilities_capacities_df["capacity"] = self.facilities_capacities_df["capacity"].apply(
            lambda x: max(x, 10)
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

        # Historical observations: the marginals of the flow log, assembled by
        # the shared ``observe`` bundle so they match the simulated set below.
        self.historical_flows_df = get_historical_flows_df(raw.trips_df, self.t0, period_len)
        self.historical_resources_df = empty_resources_obs_df()

        historical_departures_df = flows_to_departures(self.historical_flows_df)

        self.initial_inventory_df = (
            historical_departures_df.groupby(["facility_id", "commodity_category"])["quantity"]
            .max()
            .reset_index()
            .sort_values("quantity", ascending=False)
            .reset_index(drop=True)
        )
        self.initial_inventory_df["quantity"] = self.initial_inventory_df["quantity"] + 10

        self.historical_inventory_df = get_inventory_df(
            self.historical_flows_df, self.initial_inventory_df
        )
        self.historical_demand_df = historical_departures_df
        self.historical_departures_df = historical_departures_df
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
    resolved.simulated_inventory_df = get_inventory_df(
        resolved.simulated_flows_df, resolved.initial_inventory_df
    )
    simulated_departures_df = flows_to_departures(resolved.simulated_flows_df)
    resolved.simulated_demand_df = simulated_departures_df
    resolved.simulated_departures_df = simulated_departures_df
    resolved.simulated_arrivals_df = flows_to_arrivals(resolved.simulated_flows_df)
    resolved.simulated_od_matrix_df = flows_to_od_matrix(resolved.simulated_flows_df)


# ---------------------------------------------------------------------------
# Wide flow journal: every event joined to its facility attributes
# ---------------------------------------------------------------------------
#: The three facility roles a flow event names, each joined to its own copy of
#: every facility attribute (capacity, geo, inventory) under a role prefix.
_FLOW_FACILITY_ROLES = ("source", "planned_target", "realized_target")

_EARTH_RADIUS_KM = 6371.0088


def _haversine_km(
    lat1: pd.Series, lng1: pd.Series, lat2: pd.Series, lng2: pd.Series
) -> pd.Series:
    """Great-circle distance in kilometres between two coordinate columns.

    Vectorised over the rows. Any row with a missing coordinate yields ``NaN``.
    """
    lat1_r, lng1_r, lat2_r, lng2_r = (np.radians(x) for x in (lat1, lng1, lat2, lng2))
    dlat = lat2_r - lat1_r
    dlng = lng2_r - lng1_r
    h = np.sin(dlat / 2) ** 2 + np.cos(lat1_r) * np.cos(lat2_r) * np.sin(dlng / 2) ** 2
    return _EARTH_RADIUS_KM * 2 * np.arcsin(np.sqrt(h))


def _join_capacity(
    flows: pd.DataFrame, capacities: pd.DataFrame, role: str
) -> pd.DataFrame:
    """Join a facility's dock capacity to one role of every flow event.

    Adds ``{role}_capacity_total`` and ``{role}_capacity_per_commodity_cat``.
    ``facilities_capacities_df`` carries no ``commodity_category`` in the base
    scenario, so the per-commodity capacity equals the total; the branch is kept
    for the day a per-commodity capacity table exists.
    """
    id_col = f"{role}_id"

    total = capacities[["facility_id", "capacity"]].rename(
        columns={"capacity": f"{role}_capacity_total"}
    )
    flows = flows.merge(total, left_on=id_col, right_on="facility_id", how="left")
    flows = flows.drop(columns=["facility_id"])

    per_cat = f"{role}_capacity_per_commodity_cat"
    if "commodity_category" in capacities.columns:
        cat = capacities.rename(columns={"capacity": per_cat})
        flows = flows.merge(
            cat,
            left_on=[id_col, "commodity_category"],
            right_on=["facility_id", "commodity_category"],
            how="left",
        )
        flows = flows.drop(columns=["facility_id"])
    else:
        flows[per_cat] = flows[f"{role}_capacity_total"]
    return flows


def _join_geo(flows: pd.DataFrame, geo: pd.DataFrame, role: str) -> pd.DataFrame:
    """Join a facility's coordinates to one role: ``{role}_lat`` and ``{role}_lng``."""
    id_col = f"{role}_id"
    cols = geo[["facility_id", "lat", "lng"]].rename(
        columns={"lat": f"{role}_lat", "lng": f"{role}_lng"}
    )
    flows = flows.merge(cols, left_on=id_col, right_on="facility_id", how="left")
    return flows.drop(columns=["facility_id"])


def _join_inventory(flows: pd.DataFrame, inventory: pd.DataFrame, role: str) -> pd.DataFrame:
    """Join a facility's inventory before and after the event period to one role.

    ``inventory`` holds the on-hand bikes at the *end* of each period, per
    ``(period_id, facility_id, commodity_category)``. For an event in period
    ``p`` the inventory *after* it is the end-of-``p`` value, and the inventory
    *before* it is the end-of-``p-1`` value. The "before" join shifts the
    inventory period forward by one so its end-of-``p-1`` row lines up with the
    event's period ``p``. Adds ``{role}_inventory_before`` and
    ``{role}_inventory_after``.
    """
    id_col = f"{role}_id"
    keys = ["period_id", id_col, "commodity_category"]

    after = inventory.rename(
        columns={"facility_id": id_col, "quantity": f"{role}_inventory_after"}
    )
    flows = flows.merge(after, on=keys, how="left")

    before = inventory.copy()
    before["period_id"] = before["period_id"] + 1
    before = before.rename(
        columns={"facility_id": id_col, "quantity": f"{role}_inventory_before"}
    )
    flows = flows.merge(before, on=keys, how="left")
    return flows


def get_flows_wide(
    graph_data: "ResolvedModelData", flows_df: pd.DataFrame | None = None
) -> pd.DataFrame:
    """Build the wide flow journal: every event joined to its facility attributes.

    Starts from a flow journal (one row per event) and, for each of the three
    facility roles an event names -- ``source``, ``planned_target``,
    ``realized_target`` -- attaches that facility's capacity, coordinates, and
    inventory before/after the event, plus the trip's duration and distance.

    Parameters
    ----------
    graph_data : ResolvedModelData
        The resolved tables to join from: ``facilities_capacities_df``,
        ``facilities_geo_df``, and ``initial_inventory_df`` (used to rebuild the
        per-period inventory from ``flows_df`` itself, so the wide table is
        self-consistent with whichever journal is passed).
    flows_df : pandas.DataFrame, optional
        The flow journal to widen. Defaults to ``graph_data.simulated_flows_df``.

    Returns
    -------
    pandas.DataFrame
        The flow journal with these columns added, per role
        ``{source, planned_target, realized_target}``:

        - ``{role}_capacity_total``, ``{role}_capacity_per_commodity_cat``
        - ``{role}_lat``, ``{role}_lng``
        - ``{role}_inventory_before``, ``{role}_inventory_after``

        and, for the trip itself (``planned_`` / ``realized_`` pairs):

        - ``planned_duration`` (``planned_end_period - start_period``),
          ``realized_duration`` (``realized_end_period - start_period``)
        - ``planned_distance_km`` (source to planned target),
          ``realized_distance_km`` (source to realized target)
    """
    if flows_df is None:
        flows_df = graph_data.simulated_flows_df
    if flows_df is None:
        raise ValueError(
            "No flow journal to widen: pass flows_df, or run a simulation and "
            "attach_simulation first so graph_data.simulated_flows_df is set."
        )
    wide = flows_df.copy()

    inventory = get_inventory_df(flows_df, graph_data.initial_inventory_df)
    # The initial inventory is the on-hand state before period 0 (end of period
    # -1). Adding it as a period -1 row lets the "before" join resolve period-0
    # events instead of leaving them empty.
    initial = graph_data.initial_inventory_df.copy()
    initial["period_id"] = -1
    inventory = pd.concat([initial[inventory.columns], inventory], ignore_index=True)

    for role in _FLOW_FACILITY_ROLES:
        wide = _join_capacity(wide, graph_data.facilities_capacities_df, role)
        wide = _join_geo(wide, graph_data.facilities_geo_df, role)
        wide = _join_inventory(wide, inventory, role)

    wide["planned_duration"] = wide["planned_end_period"] - wide["start_period"]
    wide["realized_duration"] = wide["realized_end_period"] - wide["start_period"]
    wide["planned_distance_km"] = _haversine_km(
        wide["source_lat"], wide["source_lng"],
        wide["planned_target_lat"], wide["planned_target_lng"],
    )
    wide["realized_distance_km"] = _haversine_km(
        wide["source_lat"], wide["source_lng"],
        wide["realized_target_lat"], wide["realized_target_lng"],
    )
    return wide


def slice_flows_wide(
    wide: pd.DataFrame,
    role: str,
    facility_id: str,
    period_id: int,
    window: int,
) -> pd.DataFrame:
    """Slice the wide flow journal around one facility and one period.

    Keeps the rows where the chosen facility role equals ``facility_id`` and the
    event period is within ``window`` periods of ``period_id`` (both ends
    included: ``period_id - window <= row.period_id <= period_id + window``).
    The ``role`` picks which of the three facility roles to filter on, so the one
    function covers all three modes.

    Parameters
    ----------
    wide : pandas.DataFrame
        The wide flow journal from :func:`get_flows_wide`.
    role : {"source", "planned_target", "realized_target"}
        Which facility role to filter on. Selects the ``{role}_id`` column.
    facility_id : str
        The facility id to keep.
    period_id : int
        Centre of the period window.
    window : int
        Half-width of the period window, in periods. ``0`` keeps only
        ``period_id`` itself.

    Returns
    -------
    pandas.DataFrame
        The matching rows, in their original order.
    """
    if role not in _FLOW_FACILITY_ROLES:
        raise ValueError(f"role must be one of {_FLOW_FACILITY_ROLES}, got {role!r}")

    id_col = f"{role}_id"
    low, high = period_id - window, period_id + window
    mask = (
        (wide[id_col] == facility_id)
        & (wide["period_id"] >= low)
        & (wide["period_id"] <= high)
    )
    return wide[mask]
