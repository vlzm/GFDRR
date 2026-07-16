"""Overnight rebalancing: plan truck moves once per window, execute them per period."""

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

from .inputs import ScenarioInputs
from .mechanics import dock_up_to_capacity, free_docks
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
    """Empty bike-level rebalance plan."""
    return pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in PLAN_DTYPES.items()})


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------
@dataclasses.dataclass(frozen=True)
class RebalancingParams:
    """Settings for one rebalancing window."""

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
) -> pd.DataFrame:
    """Bikes each station should hold for the morning (peak of the running net shortfall)."""
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
        rows["net"] = sign * rows["quantity"]
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
    """``inventory - target`` per (facility, commodity); positive gives bikes, negative needs."""
    inv = inventory[[*_KEYS, "quantity"]].astype(dict.fromkeys(_KEYS, "string"))
    tgt = target[[*_KEYS, "target"]].astype(dict.fromkeys(_KEYS, "string"))
    out = inv.merge(tgt, on=_KEYS, how="outer")
    out["imbalance"] = (out["quantity"].fillna(0) - out["target"].fillna(0)).astype("int64")
    return out[[*_KEYS, "imbalance"]]


def clip_dropoffs_to_free_docks(
    imbalance: pd.DataFrame, inventory: pd.DataFrame, capacities: pd.DataFrame
) -> pd.DataFrame:
    """Cut each station's planned inflow down to its free docks (pickups untouched)."""
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
    """Trim the larger side so pickups and dropoffs move the same bike count per commodity."""

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
    """Split the imbalance into solver visits of at most ``portion_size`` bikes."""
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
    """Truck travel time in minutes between facilities (straight-line over truck speed)."""
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
    """Empty stops table."""
    return pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in _STOP_DTYPES.items()})


def solve_rebalance_vrp(
    nodes: pd.DataFrame,
    travel_minutes: pd.DataFrame,
    trucks: pd.DataFrame,
    params: RebalancingParams,
) -> pd.DataFrame:
    """Route the trucks through the pickup and dropoff nodes within the window (OR-Tools VRP)."""
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
    """Turn the solver's stops into the bike-level rebalance plan, one row per bike."""
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
    """Plan the window's truck moves; store the plan on the state (fires once per simulated day)."""

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
    ) -> SimulationState:
        """Compute the imbalance, route the trucks, store the bike-level plan."""
        if period.start_timestamp.hour != self.params.window_start_hour:
            return state
        plan = plan_rebalance(state, resolved, period, self.params, self._solver)
        return state.with_rebalance_plan(plan)


def plan_rebalance(
    state: SimulationState,
    resolved: ScenarioInputs,
    period: PeriodRow,
    params: RebalancingParams,
    solver: SolverFn = solve_rebalance_vrp,
) -> pd.DataFrame:
    """Build the bike-level rebalance plan for the window opening at ``period`` (empty if idle)."""
    target = target_inventory(
        resolved.historical_demand_df,
        resolved.historical_arrivals_df,
        resolved.periods_df,
        period.start_timestamp,
        params,
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
    """Execute the plan rows whose period has come, in three ordered rounds."""

    phase_rank = REBALANCE_RANK

    def execute(
        self,
        state: SimulationState,
        resolved: ScenarioInputs,
        period: PeriodRow,
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
    """Build this period's rebalance events and the plan still left to run."""
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
    """Dock dropped-off bikes; what does not fit goes to the truck's home depot."""
    fits, overflow = dock_up_to_capacity(due, free_docks(inventory, capacities))
    fits = fits.assign(realized_target_id=fits["planned_target_id"])
    overflow = overflow.assign(realized_target_id=overflow["resource_id"].map(home_by_resource))
    landed = pd.concat([fits, overflow], ignore_index=True)
    return rebalance_arrived_events(landed, period_id)


def _pickups_up_to_inventory(due: pd.DataFrame, inventory: pd.DataFrame) -> pd.DataFrame:
    """Keep the planned pickups the source can actually give (first rows up to bikes on hand)."""
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
    """Build the two rebalancing phases, in order."""
    return [PlanRebalancingPhase(params, solver), ApplyRebalancingPhase()]
