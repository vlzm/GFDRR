"""Overnight rebalancing: plan truck moves once per window, execute them per period.

Rebalancing moves bikes between stations by truck at night so that the morning
demand finds them (Notations.md §14). Two clocks are involved and they never
mix:

- The **solver clock** is minutes since the window started. The routing solver
  plans every truck's route on this axis, inside ``window_minutes``.
- The **simulator clock** is periods. Each planned stop is applied in the
  period its minute falls into; the sub-period detail is kept only for
  explanation.

So the plan is computed at one point of simulated time (the period that opens
the window) and executed across the following periods. The trucks never
"return to the depot at the end of each period" -- the period edge exists only
for accounting.

The flow of one window, in order:

1. :class:`PlanRebalancingPhase` (fires only in the window-opening period,
   writes no events): :func:`target_inventory` computes how many bikes each
   station should hold for the morning; :func:`station_imbalance` compares that
   with the bikes on hand; :func:`build_rebalance_nodes` splits the imbalance
   into solver visits of at most ``portion_size`` bikes;
   :func:`solve_rebalance_vrp` routes the trucks through those visits (the
   OR-Tools core); :func:`assign_bikes_to_stops` turns the routes into the
   bike-level plan stored on ``SimulationState.rebalance_plan``.
2. :class:`ApplyRebalancingPhase` (fires every period, ``phase_rank`` 3):
   executes the plan rows whose periods have come -- pickups take bikes out of
   docks (``departed``, ``flow_type="rebalance"``), dropoffs dock them
   (``arrived``). Between the two the bikes sit in ``in_transit`` like any
   riding bike.

The plan is built from the inventory at planning time, but the night demand of
the window's own periods keeps running. Execution therefore never trusts the
plan blindly: a pickup is cut down to the bikes actually on hand, and a
dropoff that finds the station full docks at the truck's home depot instead.
"""

import dataclasses
from collections.abc import Callable

import numpy as np
import pandas as pd
import structlog
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

from gbp.model import (
    REBALANCE_RANK,
    empty_flows_journal,
    haversine_km,
    occupancy_per_facility,
    rebalance_arrived_events,
    rebalance_departed_events,
)

from .config import EnvironmentConfig
from .inputs import ScenarioInputs
from .mechanics import dock_up_to_capacity, free_docks, scale_demand
from .phases import Phase
from .state import PeriodRow, SimulationState, SimulatorConfigError

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Table schemas (Notations.md §14)
# ---------------------------------------------------------------------------
#: One solver visit: at most ``portion_size`` bikes picked up or dropped at one
#: facility. ``node_type`` is ``"pickup"`` (the facility gives bikes) or
#: ``"dropoff"`` (it receives them).
NODE_COLUMNS = ["facility_id", "commodity_category", "node_type", "quantity"]

#: One row of a truck's route in the solver's answer: the truck visits
#: ``facility_id`` at ``minute`` (since window start) and picks up or drops
#: ``quantity`` bikes. ``stop_seq`` is the visit order within the truck's route.
STOP_COLUMNS = [
    "resource_id",
    "stop_seq",
    "facility_id",
    "stop_type",
    "commodity_category",
    "quantity",
    "minute",
]

#: The bike-level rebalance plan: one row per bike a truck will move.
PLAN_DTYPES = {
    "flow_id": "string",
    "resource_id": "string",
    "commodity_category": "string",
    "source_id": "string",
    "planned_target_id": "string",
    "pickup_period": "int64",
    "dropoff_period": "int64",
    "pickup_minute": "float64",
    "dropoff_minute": "float64",
}
PLAN_COLUMNS = list(PLAN_DTYPES)


def empty_rebalance_plan() -> pd.DataFrame:
    """Empty bike-level rebalance plan (the columns of Notations.md §14)."""
    return pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in PLAN_DTYPES.items()})


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------
@dataclasses.dataclass(frozen=True)
class RebalancingParams:
    """Settings for one rebalancing window (Notations.md §14).

    Attributes
    ----------
    window_start_hour : int
        Wall-clock hour that opens the window; the planning phase fires in
        each period whose start hour equals it. Default 1 (01:00).
    window_minutes : int
        How long the trucks work: every route must start and end at the depot
        within this many minutes. Default 120 (two hours).
    target_start_hour, target_end_hour : int
        The morning hours the target inventory is computed for: expected
        departures minus expected arrivals over the periods whose start hour
        is in ``[target_start_hour, target_end_hour)``, same calendar day as
        the window. Defaults 6 and 12.
    portion_size : int
        Most bikes one solver visit moves. A large imbalance is split into
        several visits so one truck does not have to serve it whole. Default 5.
    truck_speed_km_per_hour : float
        Truck speed for the straight-line travel-time estimate
        (:func:`truck_travel_minutes`). Default 25.
    stop_service_minutes : float
        Fixed minutes a truck spends at every stop (parking, opening up).
        Default 2.
    bike_service_minutes : float
        Extra minutes per bike loaded or unloaded at a stop. Default 0.5.
    drop_penalty_minutes : int
        Solver cost (in travel minutes) of skipping one visit. Set high, so
        the solver serves as many visits as fit in the window and only then
        minimizes driving. Default 10_000.
    solver_time_limit_seconds : int
        Real (wall-clock) seconds the solver may search. This is the solver's
        own running time, not simulated time. Default 10.
    """

    window_start_hour: int = 1
    window_minutes: int = 120
    target_start_hour: int = 6
    target_end_hour: int = 12
    portion_size: int = 5
    truck_speed_km_per_hour: float = 25.0
    stop_service_minutes: float = 2.0
    bike_service_minutes: float = 0.5
    drop_penalty_minutes: int = 10_000
    solver_time_limit_seconds: int = 10


# ---------------------------------------------------------------------------
# Planning: target, imbalance, nodes
# ---------------------------------------------------------------------------
_KEYS = ["facility_id", "commodity_category"]


def target_inventory(
    demand: pd.DataFrame,
    arrivals: pd.DataFrame,
    periods_df: pd.DataFrame,
    plan_start: pd.Timestamp,
    params: RebalancingParams,
    demand_scale_factor: float = 1.0,
) -> pd.DataFrame:
    """Bikes each station should hold for the morning (Notations.md §14).

    For each ``(facility, commodity)`` the morning is walked period by period:
    expected departures minus expected arrivals, added up as a running total.
    The highest point of that running total is the most bikes the station is
    ever short by, so holding that many at the window's end covers the whole
    morning. A station whose arrivals outrun its departures never runs short
    and gets target 0.

    Parameters
    ----------
    demand : pandas.DataFrame
        Expected departures per ``(period_id, facility_id, commodity_category)``
        with ``quantity`` (the historical demand).
    arrivals : pandas.DataFrame
        Expected arrivals, same shape (the historical arrivals).
    periods_df : pandas.DataFrame
        The period grid with ``period_id`` and ``start_timestamp``; picks the
        morning periods by wall-clock hour.
    plan_start : pandas.Timestamp
        Start of the window-opening period; the morning is the same calendar
        day, hours ``[params.target_start_hour, params.target_end_hour)``.
    params : RebalancingParams
        The window settings.
    demand_scale_factor : float, optional
        The run's demand multiplier; both departures and arrivals are scaled
        by it, matching the demand the run actually faces.

    Returns
    -------
    pandas.DataFrame
        ``facility_id``, ``commodity_category``, ``target`` (whole bikes,
        never negative). Facilities that never run short are absent (target 0).
    """
    empty = pd.DataFrame(
        {
            "facility_id": pd.Series(dtype="string"),
            "commodity_category": pd.Series(dtype="string"),
            "target": pd.Series(dtype="int64"),
        }
    )
    starts = periods_df["start_timestamp"]
    in_morning = (
        (starts.dt.normalize() == plan_start.normalize())
        & (starts.dt.hour >= params.target_start_hour)
        & (starts.dt.hour < params.target_end_hour)
    )
    period_ids = periods_df.loc[in_morning, "period_id"].astype("int64")
    if period_ids.empty:
        return empty

    def _signed(marginal: pd.DataFrame, sign: int) -> pd.DataFrame:
        rows = marginal[marginal["period_id"].isin(period_ids)].copy()
        rows["net"] = sign * scale_demand(rows["quantity"], demand_scale_factor)
        return rows[["period_id", *_KEYS, "net"]]

    net = pd.concat([_signed(demand, +1), _signed(arrivals, -1)], ignore_index=True)
    if net.empty:
        return empty
    matrix = net.pivot_table(
        index=_KEYS, columns="period_id", values="net", aggfunc="sum", fill_value=0
    ).reindex(columns=sorted(period_ids), fill_value=0)
    peak = matrix.cumsum(axis=1).max(axis=1).clip(lower=0)
    out = peak.rename("target").reset_index()
    out = out[out["target"] > 0].reset_index(drop=True)
    out["target"] = out["target"].astype("int64")
    return out.astype(dict.fromkeys(_KEYS, "string"))


def station_imbalance(inventory: pd.DataFrame, target: pd.DataFrame) -> pd.DataFrame:
    """``inventory - target`` per (facility, commodity) (Notations.md §14).

    Positive: the station has bikes to give (pickups happen there). Negative:
    it needs bikes (dropoffs happen there). A facility missing from either
    table counts as 0 there.
    """
    inv = inventory[[*_KEYS, "quantity"]].astype(dict.fromkeys(_KEYS, "string"))
    tgt = target[[*_KEYS, "target"]].astype(dict.fromkeys(_KEYS, "string"))
    out = inv.merge(tgt, on=_KEYS, how="outer")
    out["imbalance"] = (out["quantity"].fillna(0) - out["target"].fillna(0)).astype("int64")
    return out[[*_KEYS, "imbalance"]]


def clip_dropoffs_to_free_docks(
    imbalance: pd.DataFrame, inventory: pd.DataFrame, capacities: pd.DataFrame
) -> pd.DataFrame:
    """Cut each station's planned inflow down to its free docks.

    A station cannot take in more bikes than it has free dock slots. Docks are
    shared across commodities, so the planned inflow is totalled per facility
    with the same rule the free docks use
    (:func:`gbp.model.occupancy_per_facility`). When that total exceeds the
    facility's free docks, every commodity's share is scaled down by the same
    factor and rounded down, so the total fits. Pickups (positive imbalance)
    are untouched -- they are already bounded by the bikes on hand, because
    ``target >= 0`` implies ``imbalance <= inventory``.
    """
    out = imbalance.copy()
    need = (-out["imbalance"]).clip(lower=0)
    need_per_facility = out["facility_id"].map(occupancy_per_facility(out.assign(quantity=need)))
    allowed = out["facility_id"].map(free_docks(inventory, capacities))
    allowed = allowed.astype("float64").fillna(0.0)
    factor = (allowed / need_per_facility).where(need_per_facility > 0, 1.0).clip(upper=1.0)
    clipped = np.floor(need * factor).astype("int64")
    is_dropoff = out["imbalance"] < 0
    out.loc[is_dropoff, "imbalance"] = -clipped[is_dropoff]
    return out


def _trim_to_common_total(imbalance: pd.DataFrame) -> pd.DataFrame:
    """Trim the larger side so pickups and dropoffs move the same bike count.

    Per commodity, only ``min(total surplus, total shortage)`` bikes can
    actually move: every truck must end its route empty, so a picked-up bike
    with no station short of one (or a shortage with no bike to send) cannot
    be served. Without the trim a lone surplus of 5 against a shortage of 3
    would give the solver no same-total node subset on both sides, and it
    would move nothing. Facilities with the largest imbalance keep their
    share first.
    """

    def _cap(side: pd.DataFrame, cap: int) -> pd.Series:
        amount = side["imbalance"].abs().sort_values(ascending=False, kind="stable")
        taken_before = amount.cumsum() - amount
        kept = np.minimum(amount, (cap - taken_before).clip(lower=0))
        return kept.reindex(side.index)

    out = []
    for _, group in imbalance.groupby("commodity_category", sort=False):
        give = group[group["imbalance"] > 0]
        take = group[group["imbalance"] < 0]
        common = min(int(give["imbalance"].sum()), int(-take["imbalance"].sum()))
        trimmed = group.copy()
        trimmed.loc[give.index, "imbalance"] = _cap(give, common).astype("int64")
        trimmed.loc[take.index, "imbalance"] = (-_cap(take, common)).astype("int64")
        out.append(trimmed)
    if not out:
        return imbalance
    return pd.concat(out, ignore_index=True)


def build_rebalance_nodes(imbalance: pd.DataFrame, portion_size: int) -> pd.DataFrame:
    """Split the imbalance into solver visits of at most ``portion_size`` bikes.

    First the two sides are matched: per commodity only
    ``min(total surplus, total shortage)`` bikes can move, so the excess is
    trimmed away (:func:`_trim_to_common_total`). Then each facility's share
    is split into portions: a facility 12 bikes over its target with
    ``portion_size=5`` becomes three pickup nodes of 5, 5 and 2 bikes, so the
    solver may send different trucks to them or skip the least valuable one.
    Facilities at their target produce no node.

    Returns
    -------
    pandas.DataFrame
        :data:`NODE_COLUMNS` -- one row per node.
    """
    balanced = _trim_to_common_total(imbalance)
    nonzero = balanced[balanced["imbalance"] != 0]
    if nonzero.empty:
        return pd.DataFrame({col: pd.Series(dtype="object") for col in NODE_COLUMNS})
    total = nonzero["imbalance"].abs()
    n_nodes = -(-total // portion_size)  # whole nodes, rounded up
    rep = nonzero.loc[nonzero.index.repeat(n_nodes)].copy()
    chunk = rep.groupby(level=0).cumcount()
    rep["quantity"] = np.minimum(
        portion_size, rep["imbalance"].abs() - chunk * portion_size
    ).astype("int64")
    rep["node_type"] = np.where(rep["imbalance"] > 0, "pickup", "dropoff")
    return rep[NODE_COLUMNS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Routing: travel times and the solver
# ---------------------------------------------------------------------------
def truck_travel_minutes(
    facilities_geo_df: pd.DataFrame,
    facility_ids: list[str],
    truck_speed_km_per_hour: float,
) -> pd.DataFrame:
    """Truck travel time in minutes between the given facilities.

    Straight-line (great-circle) distance over the truck speed -- the same
    formula mode the scenario's ``haversine`` routing uses, with a truck speed
    instead of a riding speed. The scenario's ``routes`` object is *not*
    reused here even in ``osrm`` mode: its table is built with the bike
    profile and would give riding times.

    TODO: fetch a car-profile OSRM table instead (a second ``Routes`` built
    against a car-profile server), so trucks drive road distances.

    Returns
    -------
    pandas.DataFrame
        Square matrix of minutes; rows and columns are the facility ids.
    """
    sel = facilities_geo_df[facilities_geo_df["facility_id"].isin(facility_ids)]
    sel = sel[["facility_id", "lat", "lng"]]
    pairs = sel.merge(sel, how="cross", suffixes=("_from", "_to"))
    km = haversine_km(pairs["lat_from"], pairs["lng_from"], pairs["lat_to"], pairs["lng_to"])
    pairs["minutes"] = km / truck_speed_km_per_hour * 60.0
    return pairs.pivot(index="facility_id_from", columns="facility_id_to", values="minutes")


#: The solver works in whole numbers; all its times are tenths of a minute.
_MINUTE_SCALE = 10

#: The stops-table dtypes (:data:`STOP_COLUMNS`).
_STOP_DTYPES = {
    "resource_id": "string",
    "stop_seq": "int64",
    "facility_id": "string",
    "stop_type": "string",
    "commodity_category": "string",
    "quantity": "int64",
    "minute": "float64",
}


def _empty_stops() -> pd.DataFrame:
    """Empty stops table (:data:`STOP_COLUMNS`)."""
    return pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in _STOP_DTYPES.items()})


def solve_rebalance_vrp(
    nodes: pd.DataFrame,
    travel_minutes: pd.DataFrame,
    trucks: pd.DataFrame,
    params: RebalancingParams,
) -> pd.DataFrame:
    """Route the trucks through the pickup and dropoff nodes within the window.

    The core routing problem (pickup and delivery of one interchangeable
    good): every truck starts and ends empty at its home depot, a pickup node
    puts its bikes on the truck, a dropoff node takes bikes off it, the load
    never goes below zero or above the truck's capacity, and every route fits
    into ``params.window_minutes``. Nodes that do not fit are skipped at a
    high cost, so the solver serves as many as it can and only then minimizes
    driving.

    The OR-Tools model, piece by piece:

    - Locations: the first positions are the home depots (one per distinct
      depot in the fleet), the positions after them are the nodes (a facility
      appears once per node). Each truck's route starts and ends at the
      position of its own home depot. All quantities of time are integers in
      tenths of a minute (OR-Tools works in whole numbers).
    - Travel: moving from ``a`` to ``b`` costs the service time at ``a``
      (``stop_service_minutes + bike_service_minutes * quantity``; zero at
      a depot) plus the travel minutes ``a -> b``. With the service time
      charged at departure, the running total of a route at a node is that
      node's arrival minute.
    - A "minutes" dimension caps every route at ``window_minutes``, return
      to the home depot included.
    - One load dimension per commodity, bounded ``[0, truck capacity]`` and
      forced to 0 at the route's end: a truck can only drop bikes it picked
      up, of the same commodity, and never keeps bikes at the end. With more
      than one commodity a shared total-load dimension caps the combined
      load at the truck's capacity.
    - Every node may be skipped at ``drop_penalty_minutes``; the penalty is
      far above any travel cost, so skipping is a last resort.
    - Search: cheapest-arc first solution, then guided local search until
      ``solver_time_limit_seconds`` of real time.

    Parameters
    ----------
    nodes : pandas.DataFrame
        The solver visits (:data:`NODE_COLUMNS`, from
        :func:`build_rebalance_nodes`).
    travel_minutes : pandas.DataFrame
        Square matrix of truck travel minutes over the involved facilities,
        home depots included (from :func:`truck_travel_minutes`).
    trucks : pandas.DataFrame
        The fleet: ``resource_id``, ``capacity`` (bikes per truck),
        ``home_facility_id`` (the depot the truck starts from and returns to).
    params : RebalancingParams
        Window length, service times, drop penalty, solver time limit.

    Returns
    -------
    pandas.DataFrame
        The stops table (:data:`STOP_COLUMNS`): each truck's visits in order,
        with the arrival ``minute`` of every stop. Skipped nodes simply do not
        appear.
    """
    if nodes.empty:
        return _empty_stops()

    # Positions: 0..k-1 = the distinct home depots, then one per node row.
    # Times in tenths of a minute.
    depots = list(dict.fromkeys(trucks["home_facility_id"]))
    n_depots = len(depots)
    facilities = [*depots, *nodes["facility_id"].tolist()]
    minutes = travel_minutes.loc[facilities, facilities].to_numpy(dtype="float64")
    quantities = [0] * n_depots + nodes["quantity"].astype(int).tolist()
    service = np.array(
        [0.0] * n_depots
        + [
            params.stop_service_minutes + params.bike_service_minutes * q
            for q in quantities[n_depots:]
        ]
    )
    transit = np.rint((service[:, None] + minutes) * _MINUTE_SCALE).astype("int64")

    depot_position = {depot: position for position, depot in enumerate(depots)}
    starts = [depot_position[home] for home in trucks["home_facility_id"]]
    manager = pywrapcp.RoutingIndexManager(len(facilities), len(trucks), starts, starts)
    routing = pywrapcp.RoutingModel(manager)

    def transit_callback(from_index: int, to_index: int) -> int:
        return int(transit[manager.IndexToNode(from_index), manager.IndexToNode(to_index)])

    transit_id = routing.RegisterTransitCallback(transit_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_id)
    horizon = int(params.window_minutes * _MINUTE_SCALE)
    routing.AddDimension(transit_id, 0, horizon, True, "minutes")
    time_dimension = routing.GetDimensionOrDie("minutes")

    # Load: +quantity at a pickup, -quantity at a dropoff; every truck ends empty.
    capacities = trucks["capacity"].astype(int).tolist()

    def _add_load_dimension(name: str, deltas: list[int]) -> None:
        callback_id = routing.RegisterUnaryTransitCallback(
            lambda index, d=tuple(deltas): int(d[manager.IndexToNode(index)])
        )
        routing.AddDimensionWithVehicleCapacity(callback_id, 0, capacities, True, name)
        dimension = routing.GetDimensionOrDie(name)
        for vehicle in range(len(trucks)):
            dimension.CumulVar(routing.End(vehicle)).SetRange(0, 0)

    signs = [0] * n_depots + [1 if t == "pickup" else -1 for t in nodes["node_type"]]
    deltas = [sign * quantity for sign, quantity in zip(signs, quantities, strict=True)]
    commodities = list(dict.fromkeys(nodes["commodity_category"]))
    for commodity in commodities:
        of_commodity = [False] * n_depots + [c == commodity for c in nodes["commodity_category"]]
        _add_load_dimension(
            f"load_{commodity}",
            [delta if mine else 0 for delta, mine in zip(deltas, of_commodity, strict=True)],
        )
    if len(commodities) > 1:
        _add_load_dimension("load_total", deltas)

    # Skipping a node is allowed but costs far more than any driving.
    penalty = int(params.drop_penalty_minutes * _MINUTE_SCALE)
    for position in range(n_depots, len(facilities)):
        routing.AddDisjunction([manager.NodeToIndex(position)], penalty)

    search = pywrapcp.DefaultRoutingSearchParameters()
    search.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    search.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )
    search.time_limit.FromSeconds(params.solver_time_limit_seconds)
    solution = routing.SolveWithParameters(search)
    if solution is None:
        return _empty_stops()

    # Walk each truck's route and emit one stops row per visited node.
    rows: list[dict[str, object]] = []
    for vehicle in range(len(trucks)):
        resource_id = trucks["resource_id"].iloc[vehicle]
        stop_seq = 0
        index = routing.Start(vehicle)
        while not routing.IsEnd(index):
            position = manager.IndexToNode(index)
            if position >= n_depots:
                node = nodes.iloc[position - n_depots]
                rows.append(
                    {
                        "resource_id": resource_id,
                        "stop_seq": stop_seq,
                        "facility_id": node["facility_id"],
                        "stop_type": node["node_type"],
                        "commodity_category": node["commodity_category"],
                        "quantity": int(node["quantity"]),
                        "minute": solution.Value(time_dimension.CumulVar(index)) / _MINUTE_SCALE,
                    }
                )
                stop_seq += 1
            index = solution.Value(routing.NextVar(index))
    if not rows:
        return _empty_stops()
    return pd.DataFrame(rows, columns=STOP_COLUMNS)


def assign_bikes_to_stops(
    stops: pd.DataFrame, window_period_id: int, minutes_per_period: int
) -> pd.DataFrame:
    """Turn the solver's stops into the bike-level rebalance plan.

    Walks each truck's route in visit order and matches bikes to stops: a
    pickup puts its bikes on the truck, a dropoff hands over the bikes that
    were picked up earliest (per truck and commodity). Each handed-over bike
    becomes one plan row with its pickup and dropoff stop. The solver's
    minutes also become simulator periods here:
    ``period = window_period_id + minute // minutes_per_period``.

    Two asserts reject a malformed ``stops`` table: a dropoff larger than the
    bikes on the truck, and bikes still on a truck at its route's end. A
    solver that respects its load bounds can produce neither.

    Returns
    -------
    pandas.DataFrame
        The plan (:data:`PLAN_COLUMNS`), one row per bike.
    """
    rows: list[dict[str, object]] = []
    # Bikes currently on each truck: (resource, commodity) -> [(source, minute), ...]
    on_truck: dict[tuple[str, str], list[tuple[str, float]]] = {}
    ordered = stops.sort_values(["resource_id", "stop_seq"], kind="stable")
    for stop in ordered.itertuples(index=False):
        key = (stop.resource_id, stop.commodity_category)
        if stop.stop_type == "pickup":
            boarding = [(stop.facility_id, stop.minute)] * int(stop.quantity)
            on_truck.setdefault(key, []).extend(boarding)
            continue
        loaded = on_truck.get(key, [])
        # The solver's load bounds guarantee a truck never drops more than it
        # carries; a stops table that does is malformed.
        assert len(loaded) >= int(stop.quantity), "dropoff exceeds the bikes on the truck"
        for _ in range(int(stop.quantity)):
            source_id, pickup_minute = loaded.pop(0)
            rows.append(
                {
                    "flow_id": f"rb_{window_period_id}_{len(rows)}",
                    "resource_id": stop.resource_id,
                    "commodity_category": stop.commodity_category,
                    "source_id": source_id,
                    "planned_target_id": stop.facility_id,
                    "pickup_period": window_period_id + int(pickup_minute // minutes_per_period),
                    "dropoff_period": window_period_id + int(stop.minute // minutes_per_period),
                    "pickup_minute": float(pickup_minute),
                    "dropoff_minute": float(stop.minute),
                }
            )
    # Every route ends at the depot empty (the load dimension ends at 0), so
    # every picked-up bike has found its dropoff row above.
    assert not any(on_truck.values()), "bikes left on a truck at the end of its route"
    if not rows:
        return empty_rebalance_plan()
    return pd.DataFrame(rows, columns=PLAN_COLUMNS).astype(PLAN_DTYPES)


#: The solver seam: PlanRebalancingPhase calls any function with this shape
#: (nodes, travel_minutes, trucks, params) -> stops.
SolverFn = Callable[[pd.DataFrame, pd.DataFrame, pd.DataFrame, RebalancingParams], pd.DataFrame]


# ---------------------------------------------------------------------------
# The two phases
# ---------------------------------------------------------------------------
class PlanRebalancingPhase(Phase):
    """Plan the window's truck moves; store the plan on the state.

    Fires only in a period whose wall-clock start hour is
    ``params.window_start_hour`` (once per simulated day); every other period
    passes through untouched. Writes no events and moves no inventory -- the
    plan is a decision, and :class:`ApplyRebalancingPhase` applies it.

    The planning inventory is the state *after* this period's own user phases
    (this phase runs later in the phase list), so the freshest picture the
    simulator has. The window's later night demand can still invalidate parts
    of the plan; execution cuts those parts down (see the module docstring).

    When one side is missing -- no station is short, or none has bikes to
    give -- the phase stores an empty plan and never calls the solver. A
    broken truck setup raises ``SimulatorConfigError`` instead of being
    planned around: no truck at all, a truck without ``home_facility_id``,
    or a home depot missing from the facility tables.

    Parameters
    ----------
    params : RebalancingParams
        The window settings.
    solver : SolverFn, optional
        The routing solver. Defaults to :func:`solve_rebalance_vrp`; tests
        inject a hand-written stand-in here.
    """

    # Writes no events; the rank only places the phase in the ordered list.
    phase_rank = REBALANCE_RANK

    def __init__(self, params: RebalancingParams, solver: SolverFn = solve_rebalance_vrp) -> None:
        self.params = params
        self._solver = solver

    def execute(
        self,
        state: SimulationState,
        resolved: ScenarioInputs,
        period: PeriodRow,
        config: EnvironmentConfig,
    ) -> SimulationState:
        """Compute the imbalance, route the trucks, store the bike-level plan."""
        if period.start_timestamp.hour != self.params.window_start_hour:
            return state
        plan = plan_rebalance(
            state, resolved, period, self.params, config.demand_scale_factor, self._solver
        )
        return state.with_rebalance_plan(plan)


def plan_rebalance(
    state: SimulationState,
    resolved: ScenarioInputs,
    period: PeriodRow,
    params: RebalancingParams,
    demand_scale_factor: float,
    solver: SolverFn = solve_rebalance_vrp,
) -> pd.DataFrame:
    """Build the bike-level rebalance plan for the window opening at ``period``.

    The ordered core of :class:`PlanRebalancingPhase`, split out so a test can
    call it with a scripted ``solver`` and assert on the plan without running
    the engine. The scheduling guard (fire only in the window-opening period)
    stays in the phase; this function assumes the window is open.

    The sequence: target inventory -> imbalance -> clip dropoffs to free docks
    -> solver visits. When one side is missing (no station is short, or none has
    bikes to give) the plan is empty and the solver is never called. A broken
    truck setup raises :class:`SimulatorConfigError` (see the class docstring).

    Returns the plan (empty when nothing moves).
    """
    target = target_inventory(
        resolved.historical_demand_df,
        resolved.historical_arrivals_df,
        resolved.periods_df,
        period.start_timestamp,
        params,
        demand_scale_factor,
    )
    imbalance = station_imbalance(state.state_inventory_df, target)
    imbalance = clip_dropoffs_to_free_docks(
        imbalance, state.state_inventory_df, resolved.facilities_capacities_df
    )
    nodes = build_rebalance_nodes(imbalance, params.portion_size)
    sides = set(nodes["node_type"].unique()) if not nodes.empty else set()
    if sides != {"pickup", "dropoff"}:
        # Nothing to move: no station is short, or none has bikes to give.
        return empty_rebalance_plan()

    trucks = resolved.resources_capacities_df.merge(
        resolved.resources_df[["resource_id", "home_facility_id"]], on="resource_id"
    )
    if trucks.empty:
        raise SimulatorConfigError("rebalancing needs at least one truck")
    if trucks["home_facility_id"].isna().any():
        no_home = trucks.loc[trucks["home_facility_id"].isna(), "resource_id"].tolist()
        raise SimulatorConfigError(f"trucks with no home depot: {no_home}")
    homes = list(dict.fromkeys(trucks["home_facility_id"]))
    known = set(resolved.facilities_geo_df["facility_id"])
    unknown = [home for home in homes if home not in known]
    if unknown:
        raise SimulatorConfigError(f"home depots missing from the facility tables: {unknown}")

    facility_ids = [*homes, *nodes["facility_id"].unique()]
    travel = truck_travel_minutes(
        resolved.facilities_geo_df, facility_ids, params.truck_speed_km_per_hour
    )
    stops = solver(nodes, travel, trucks, params)
    minutes_per_period = int(resolved.period_len / pd.Timedelta(minutes=1))
    plan = assign_bikes_to_stops(stops, period.period_id, minutes_per_period)
    log.debug("rebalancing_planned", plan_rows=len(plan), trucks=len(trucks))
    return plan


class ApplyRebalancingPhase(Phase):
    """Execute the plan rows whose period has come (``phase_rank`` 3).

    Runs every period, after the three user-trip phases; a period with no due
    plan rows and no rebalance bike in ``in_transit`` passes through
    unchanged. The work happens in three ordered rounds (each its own
    inventory step):

    - round 0 -- dock the dropoffs due now for bikes picked up in an earlier
      period (they were waiting in ``in_transit``);
    - round 1 -- this period's pickups, cut down to the bikes actually on
      hand at each source; a cut pickup disappears from the plan and the bike
      stays where the night demand left it;
    - round 2 -- dock the dropoffs of bikes picked up within this same period.

    A dropoff docks at its planned station while it has free docks; bikes that
    do not fit dock at the truck's home depot instead (the truck could not
    unload and takes them back). The depot's own capacity is not checked -- it
    is the parking of last resort.
    """

    phase_rank = REBALANCE_RANK

    def execute(
        self,
        state: SimulationState,
        resolved: ScenarioInputs,
        period: PeriodRow,
        config: EnvironmentConfig,
    ) -> SimulationState:
        """Apply this period's pickups and dropoffs; return the next state."""
        events, remaining_plan = apply_rebalance(state, resolved, period.period_id)
        new_state = state
        if not events.empty:
            log.debug("rebalancing_applied", events=len(events))
            new_state = new_state.apply_step_events(events, self.phase_rank)
        return new_state.with_rebalance_plan(remaining_plan)


def apply_rebalance(
    state: SimulationState,
    resolved: ScenarioInputs,
    t: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build this period's rebalance events and the plan still left to run.

    The ordered core of :class:`ApplyRebalancingPhase`, split out so a test can
    call it on a hand-built state and assert on the ordering without the engine.
    The three rounds run in order, each seeing the docks the earlier rounds took
    or freed (``inventory_now`` reads the state with the rounds built so far
    applied):

    - round 0 -- dock the dropoffs due now for bikes picked up earlier;
    - round 1 -- this period's pickups, cut to the bikes actually on hand;
    - round 2 -- dock the dropoffs of bikes picked up within this period.

    Returns ``(events, remaining_plan)``: the event batch to write, carrying a
    ``phase_round`` per round (empty when nothing happens), and the plan rows
    whose pickup period is still ahead. The caller writes the batch once through
    ``apply_step_events``, which turns each round into its own inventory step.
    """
    plan = state.rebalance_plan
    in_transit = state.in_transit
    on_truck = in_transit["flow_type"] == "rebalance"
    if plan.empty and not on_truck.any():
        return empty_flows_journal(), plan

    home_by_resource = resolved.resources_df.set_index("resource_id")["home_facility_id"]
    capacities = resolved.facilities_capacities_df
    batches: list[pd.DataFrame] = []

    def inventory_now() -> pd.DataFrame:
        # Each round's docking and pickup decisions must see the docks the
        # earlier rounds took or freed, so the inventory for the next decision
        # is read from the state with the rounds built so far applied. The real
        # write happens once, in the caller's apply_step_events.
        if not batches:
            return state.state_inventory_df
        return state.inventory_after_events(pd.concat(batches, ignore_index=True))

    # Round 0 -- dropoffs due now for bikes picked up in an earlier period.
    due_previous = in_transit[on_truck & (in_transit["planned_end_period"] == t)]
    if not due_previous.empty:
        arrived = _dock_dropoffs(due_previous, inventory_now(), capacities, home_by_resource, t)
        batches.append(arrived.assign(phase_round=0))

    # Round 1 -- this period's pickups, cut to the bikes actually on hand.
    due_same = None
    if not plan.empty:
        due_pickups = plan[plan["pickup_period"] == t]
        plan = plan[plan["pickup_period"] > t]
        executed = _pickups_up_to_inventory(due_pickups, inventory_now())
        if not executed.empty:
            departed = rebalance_departed_events(
                pd.DataFrame(
                    {
                        "flow_id": executed["flow_id"],
                        "source_id": executed["source_id"],
                        "planned_target_id": executed["planned_target_id"],
                        "commodity_category": executed["commodity_category"],
                        "resource_id": executed["resource_id"],
                        "start_period": t,
                        "planned_end_period": executed["dropoff_period"],
                    }
                )
            )
            batches.append(departed.assign(phase_round=1))
            due_same = departed[departed["planned_end_period"] == t]

    # Round 2 -- dock the dropoffs of bikes picked up within this period.
    if due_same is not None and not due_same.empty:
        arrived = _dock_dropoffs(due_same, inventory_now(), capacities, home_by_resource, t)
        batches.append(arrived.assign(phase_round=2))

    if not batches:
        return empty_flows_journal(), plan
    return pd.concat(batches, ignore_index=True), plan


def _dock_dropoffs(
    due: pd.DataFrame,
    inventory: pd.DataFrame,
    capacities: pd.DataFrame,
    home_by_resource: pd.Series,
    period_id: int,
) -> pd.DataFrame:
    """Dock dropped-off bikes; what does not fit goes to the truck's home depot.

    The same docking rule as the user phases (:func:`dock_up_to_capacity`) at
    the planned station; the overflow's ``realized_target_id`` becomes the
    truck's home depot. ``inventory`` is the caller's decision input -- the
    state with the earlier rounds applied. Returns the ``arrived`` events.
    """
    fits, overflow = dock_up_to_capacity(due, free_docks(inventory, capacities))
    fits = fits.assign(realized_target_id=fits["planned_target_id"])
    overflow = overflow.assign(realized_target_id=overflow["resource_id"].map(home_by_resource))
    landed = pd.concat([fits, overflow], ignore_index=True)
    return rebalance_arrived_events(landed, period_id)


def _pickups_up_to_inventory(due: pd.DataFrame, inventory: pd.DataFrame) -> pd.DataFrame:
    """Keep the planned pickups the source can actually give.

    Within each ``(source, commodity)`` the first plan rows (in pickup-minute
    order) are kept, up to the bikes on hand; the rest are cut from the plan --
    their bikes stay where the night demand left them, and their dropoffs never
    happen. The same cumulative-count pattern as :func:`dock_up_to_capacity`,
    keyed by source and commodity.
    """
    if due.empty:
        return due
    ordered = due.sort_values(["pickup_minute", "flow_id"], kind="stable")
    on_hand = inventory.set_index(_KEYS)["quantity"]
    rank = ordered.groupby(["source_id", "commodity_category"]).cumcount()
    pair = pd.MultiIndex.from_frame(ordered[["source_id", "commodity_category"]])
    available = pd.Series(on_hand.reindex(pair).to_numpy(), index=ordered.index).fillna(0)
    return ordered[rank < available]


def rebalancing_phases(
    params: RebalancingParams, solver: SolverFn = solve_rebalance_vrp
) -> list[Phase]:
    """Build the two rebalancing phases, in order, to append to ``canonical_phases()``."""
    return [PlanRebalancingPhase(params, solver), ApplyRebalancingPhase()]
