"""RebalancerTask: multi-stop PDP-based truck rebalancing.

Produces dispatches that move bikes from over-utilized stations (sources) to
under-utilized stations (destinations) by solving a Pickup-and-Delivery
Problem (PDP) with OR-Tools.  The output respects the standard
:data:`DISPATCH_COLUMNS` schema so it plugs straight into ``DispatchPhase``
without touching the lifecycle.

The algorithm is a port of the deprecated ``gbp/rebalancer/`` prototype:

- ``_compute_imbalance``        — salvaged from ``gbp/rebalancer/demand.py``.
- :class:`~gbp.consumers.simulator.tasks.rebalancer_planner.IntervalOverlapPlanner`
  — pickup-delivery matching, salvaged from
  ``gbp/rebalancer/dataloader.py``.
- ``_solve_pdp`` and ``_extract_pdp_solution`` — salvaged from
  ``gbp/rebalancer/routing/{vrp,postprocessing}.py``.

Multi-stop semantics: the rows produced for one truck form a chain rooted at
the truck's ``current_facility_id``.  ``DispatchPhase``'s lifecycle validator
treats the chain as a single engaged route and the truck is released exactly
once when the whole route arrives.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

import pandas as pd
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

from gbp.consumers.simulator._period_helpers import period_duration_hours
from gbp.consumers.simulator.distance import (
    build_edge_distance_map,
    build_node_distance_matrix,
)
from gbp.consumers.simulator.task import DISPATCH_COLUMNS
from gbp.consumers.simulator.tasks.rebalancer_planner import (
    IntervalOverlapPlanner,
    Planner,
)
from gbp.core.enums import ModalType, ResourceStatus

if TYPE_CHECKING:
    from gbp.consumers.simulator.state import PeriodRow, SimulationState
    from gbp.core.model import ResolvedModelData


# ---------------------------------------------------------------------------
# Public class
# ---------------------------------------------------------------------------


class RebalancerTask:
    """Multi-stop PDP rebalancer that emits truck routes as dispatches.

    Reuses the OR-Tools PDP idea from the deprecated ``gbp/rebalancer/``
    prototype.  Returns a DataFrame with exactly :data:`DISPATCH_COLUMNS`.
    Each row corresponds to one pickup-delivery pair on a truck's route;
    rows belonging to the same truck share an ``arrival_period``.

    Parameters
    ----------
    min_threshold
        Stations below this utilization are destinations.
        Default is ``0.3``.
    max_threshold
        Stations above this utilization are sources.
        Default is ``0.7``.
    time_limit_seconds
        OR-Tools solver time budget.  Default is ``30``.
    pdp_random_seed
        Forwarded to ``RoutingSearchParameters.random_seed``
        so baseline-vs-treatment comparisons are reproducible.
        Default is ``42``.
    period_duration_hours
        Hours per period.  ``None`` means derive from
        ``resolved.periods`` at run time.  Default is ``None``.
    truck_speed_kmh
        Average truck speed used to convert route distance into hours.
        Default is ``30.0``.
    truck_resource_category
        Resource category id of rebalancing trucks in ``state.resources``.
        Default is ``"rebalancing_truck"``.
    commodity_category
        Commodity moved by the rebalancer.
        Default is ``"working_bike"``.
    modal_type
        Modal type stamped on every dispatch row.
        Default is ``ModalType.ROAD.value``.
    planner
        Strategy for turning per-station imbalance into PDP
        pickup-delivery pairs.  ``None`` falls back to
        :class:`IntervalOverlapPlanner` (geography-blind cumulative
        interval matching).  Default is ``None``.
    """

    name: str = "rebalancer"

    def __init__(
        self,
        *,
        min_threshold: float = 0.3,
        max_threshold: float = 0.7,
        time_limit_seconds: int = 30,
        pdp_random_seed: int = 42,
        period_duration_hours: float | None = None,
        truck_speed_kmh: float = 30.0,
        truck_resource_category: str = "rebalancing_truck",
        commodity_category: str = "working_bike",
        modal_type: str = ModalType.ROAD.value,
        planner: Planner | None = None,
    ) -> None:
        self.min_threshold = min_threshold
        self.max_threshold = max_threshold
        self.time_limit_seconds = time_limit_seconds
        self.pdp_random_seed = pdp_random_seed
        self.period_duration_hours = period_duration_hours
        self.truck_speed_kmh = truck_speed_kmh
        self.truck_resource_category = truck_resource_category
        self.commodity_category = commodity_category
        self.modal_type = modal_type
        self.planner: Planner = planner if planner is not None else IntervalOverlapPlanner()

    # ── public entry point ────────────────────────────────────────────

    def run(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
    ) -> pd.DataFrame:
        """Build a DataFrame of multi-stop truck dispatches for *period*.

        Returns an empty :data:`DISPATCH_COLUMNS` frame whenever any
        early-exit condition is hit (no available trucks, no imbalance,
        solver failure, no depot).

        Parameters
        ----------
        state
            Current simulation state with inventory and resources.
        resolved
            Resolved model data with facilities, distance matrix, etc.
        period
            Current period descriptor.

        Returns
        -------
        pd.DataFrame
            Dispatches with ``DISPATCH_COLUMNS`` schema.
        """
        empty = _empty_dispatches()

        # Step 0 — early exit on no available trucks (R9).
        available_trucks = state.resources[
            (state.resources["resource_category"] == self.truck_resource_category)
            & (state.resources["status"] == ResourceStatus.AVAILABLE.value)
        ].copy()
        if available_trucks.empty:
            return empty

        # Step 1 — per-station inventory + capacity.
        df_nodes = _build_node_state(state, resolved, self.commodity_category)
        if df_nodes is None or df_nodes.empty:
            return empty

        # Step 2 — utilization, sources, destinations.
        df_nodes, sources, destinations = _compute_imbalance(
            df_nodes, self.min_threshold, self.max_threshold,
        )
        if sources.empty or destinations.empty:
            return empty

        # Step 3 — pick depot.
        depot_row = _pick_depot(resolved)
        if depot_row is None:
            return empty
        depot_id, depot_lat, depot_lon = depot_row

        # Truck capacity from resolved.resource_categories.
        truck_capacity = _truck_capacity(resolved, self.truck_resource_category)
        if truck_capacity is None or truck_capacity <= 0:
            return empty

        # Step 4 — pairs (matching strategy is pluggable via self.planner).
        pairs = self.planner.plan(
            sources, destinations, truck_capacity, resolved.distance_matrix,
        )
        if not pairs:
            return empty

        # Step 5 — build PDP model.
        n_trucks = len(available_trucks)
        model = _build_pdp_model(
            pairs=pairs,
            depot_id=depot_id,
            depot_lat=depot_lat,
            depot_lon=depot_lon,
            distance_matrix=resolved.distance_matrix,
            n_trucks=n_trucks,
            truck_capacity=int(truck_capacity),
        )

        # Step 6 — solve.
        solution = _solve_pdp(
            model,
            time_limit_seconds=self.time_limit_seconds,
            random_seed=self.pdp_random_seed,
        )
        if solution is None or not solution["routes"]:
            return empty

        # Step 7 — translate routes into DISPATCH_COLUMNS rows.
        period_duration = self._resolve_period_duration(resolved)
        truck_ids = available_trucks["resource_id"].astype(str).tolist()
        truck_loc = dict(
            zip(
                available_trucks["resource_id"].astype(str),
                available_trucks["current_facility_id"].astype(str),
                strict=True,
            )
        )

        rows: list[dict[str, Any]] = []
        for route_info in solution["routes"]:
            truck_idx = int(route_info["resource_id"])
            if truck_idx >= len(truck_ids):
                continue
            truck_id = truck_ids[truck_idx]
            route_pairs = _route_to_pairs(route_info["route"], pairs)
            if not route_pairs:
                continue
            distance_km = float(route_info["distance"])
            arrival_period = period.period_index + max(
                1,
                math.ceil(
                    (distance_km / self.truck_speed_kmh) / period_duration
                ),
            )
            for source_id, target_id, qty in route_pairs:
                rows.append({
                    "source_id": source_id,
                    "target_id": target_id,
                    "commodity_category": self.commodity_category,
                    "quantity": int(qty),
                    "resource_id": truck_id,
                    "modal_type": self.modal_type,
                    "arrival_period": int(arrival_period),
                })

        if not rows:
            return empty

        out = pd.DataFrame(rows, columns=DISPATCH_COLUMNS)

        # Step 8 — belt-and-braces: every emitted resource_id is in the
        # available truck pool.  Pickup-delivery rows do NOT form a physical
        # traversal chain (a row's source is a pickup station, not the
        # previous row's target), so chain-rooted-at-truck validation does
        # not apply here.  ``DispatchPhase`` enforces resource availability
        # downstream — this assertion just catches solver bugs that would
        # produce phantom resource ids.
        _assert_resource_pre_assigned(out, set(truck_loc))

        return out

    # ── helpers ───────────────────────────────────────────────────────

    def _resolve_period_duration(self, resolved: ResolvedModelData) -> float:
        """Return period duration in hours.

        Honours an explicit constructor override; otherwise delegates to the
        shared :func:`gbp.consumers.simulator._period_helpers.period_duration_hours`
        helper.

        Parameters
        ----------
        resolved
            Resolved model data used to derive period duration when no
            explicit override was set in the constructor.

        Returns
        -------
        float
            Period duration in hours.
        """
        if self.period_duration_hours is not None:
            return float(self.period_duration_hours)
        return period_duration_hours(resolved)


# ---------------------------------------------------------------------------
# Module-level helpers (private)
# ---------------------------------------------------------------------------


def _empty_dispatches() -> pd.DataFrame:
    """Return an empty DataFrame with exactly the DISPATCH_COLUMNS schema.

    Returns
    -------
    pd.DataFrame
        Empty DataFrame with ``DISPATCH_COLUMNS`` columns.
    """
    return pd.DataFrame(columns=DISPATCH_COLUMNS)


def _build_node_state(
    state: SimulationState,
    resolved: ResolvedModelData,
    commodity_category: str,
) -> pd.DataFrame | None:
    """Build a per-station node-state frame.

    Returns a DataFrame with columns ``[node_id, latitude, longitude,
    quantity, inventory_capacity]``.

    Capacity comes from ``resolved.attributes["operation_capacity"]``
    filtered to ``operation_type == "storage"`` and matching
    *commodity_category*.  When the attribute is absent or yields no rows
    for the relevant slice, falls back to ``per_facility_quantity.max() * 2``.

    Parameters
    ----------
    state
        Current simulation state with inventory data.
    resolved
        Resolved model data with facilities and attributes.
    commodity_category
        Commodity to aggregate inventory for.

    Returns
    -------
    pd.DataFrame or None
        Per-station node-state frame, or ``None`` when no usable station
        rows can be assembled.
    """
    inv = state.inventory[
        state.inventory["commodity_category"] == commodity_category
    ].copy()
    if inv.empty:
        return None
    qty = inv.groupby("facility_id", as_index=False)["quantity"].sum()

    facilities = resolved.facilities
    if facilities is None or facilities.empty:
        return None
    stations = facilities[facilities["facility_type"] == "station"].copy()
    if stations.empty:
        return None

    qty = qty[qty["facility_id"].isin(stations["facility_id"])]
    if qty.empty:
        return None

    capacity_series = _facility_capacity(resolved, commodity_category)
    if capacity_series is None or capacity_series.empty:
        # Fallback: 2x the maximum observed quantity, per facility.
        max_qty = (
            state.inventory.groupby("facility_id")["quantity"].max() * 2
        )
        capacity_series = max_qty.astype(float)

    df = qty.merge(
        stations[["facility_id", "lat", "lon"]],
        on="facility_id",
        how="left",
    )
    df = df.rename(columns={
        "facility_id": "node_id",
        "lat": "latitude",
        "lon": "longitude",
    })
    df["inventory_capacity"] = (
        df["node_id"].map(capacity_series).astype("float64")
    )
    df = df.dropna(subset=["latitude", "longitude", "inventory_capacity"])
    df = df[df["inventory_capacity"] > 0].copy()
    if df.empty:
        return None
    df["quantity"] = df["quantity"].astype(int)
    df["inventory_capacity"] = df["inventory_capacity"].astype(int)
    return df.reset_index(drop=True)


def _facility_capacity(
    resolved: ResolvedModelData, commodity_category: str,
) -> pd.Series | None:
    """Return per-facility storage capacity for a commodity category.

    Parameters
    ----------
    resolved
        Resolved model data containing attributes.
    commodity_category
        Commodity to look up storage capacity for.

    Returns
    -------
    pd.Series or None
        Series indexed by ``facility_id`` with summed capacity, or
        ``None`` when the attribute is absent or has no matching rows.
    """
    if "operation_capacity" not in resolved.attributes:
        return None
    cap_data = resolved.attributes.get("operation_capacity").data
    if cap_data is None or cap_data.empty:
        return None
    storage = cap_data[cap_data["operation_type"] == "storage"]
    if "commodity_category" in storage.columns:
        storage = storage[storage["commodity_category"] == commodity_category]
    if storage.empty:
        return None
    return storage.groupby("facility_id")["capacity"].sum().astype(float)


def _compute_imbalance(
    df: pd.DataFrame, min_threshold: float, max_threshold: float,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Compute utilization and split nodes into sources and destinations.

    Salvaged from ``gbp/rebalancer/demand.py:compute_utilization_and_balance``,
    inlined to keep the Task self-contained.

    Parameters
    ----------
    df
        Node-state frame with ``quantity`` and ``inventory_capacity`` columns.
    min_threshold
        Utilization below which a station is a destination.
    max_threshold
        Utilization above which a station is a source.

    Returns
    -------
    tuple of (pd.DataFrame, pd.DataFrame, pd.DataFrame)
        ``(annotated_df, sources, destinations)`` where *annotated_df*
        has added ``utilization``, ``target_count``, and ``balance``
        columns; *sources* has an ``excess`` column; *destinations* has
        a ``deficit`` column.
    """
    df = df.copy()
    df["utilization"] = df["quantity"] / df["inventory_capacity"]
    target_utilization = (min_threshold + max_threshold) / 2
    df["target_count"] = df["inventory_capacity"] * target_utilization
    df["balance"] = df["quantity"] - df["target_count"]

    sources = df[
        (df["utilization"] > max_threshold) & (df["balance"] > 1)
    ].copy()
    sources["excess"] = sources["balance"].astype(int)

    destinations = df[
        (df["utilization"] < min_threshold) & (df["balance"] < -1)
    ].copy()
    destinations["deficit"] = (-destinations["balance"]).astype(int)

    return df, sources, destinations


def _pick_depot(
    resolved: ResolvedModelData,
) -> tuple[str, float, float] | None:
    """Pick the first depot with valid coordinates.

    Parameters
    ----------
    resolved
        Resolved model data containing facilities.

    Returns
    -------
    tuple of (str, float, float) or None
        ``(facility_id, lat, lon)`` of the first depot, or ``None``
        when no depot with valid coordinates exists.
    """
    facilities = resolved.facilities
    if facilities is None or facilities.empty:
        return None
    depots = facilities[facilities["facility_type"] == "depot"].dropna(
        subset=["lat", "lon"],
    )
    if depots.empty:
        return None
    row = depots.iloc[0]
    return str(row["facility_id"]), float(row["lat"]), float(row["lon"])


def _truck_capacity(
    resolved: ResolvedModelData, truck_resource_category: str,
) -> float | None:
    """Look up ``base_capacity`` for the truck resource category.

    Parameters
    ----------
    resolved
        Resolved model data containing resource categories.
    truck_resource_category
        Resource category id to look up.

    Returns
    -------
    float or None
        Base capacity of the truck category, or ``None`` if not found.
    """
    rc = resolved.resource_categories
    if rc is None or rc.empty:
        return None
    match = rc[rc["resource_category_id"] == truck_resource_category]
    if match.empty:
        return None
    return float(match.iloc[0]["base_capacity"])


def _build_pdp_model(
    *,
    pairs: list[dict[str, Any]],
    depot_id: str,
    depot_lat: float,
    depot_lon: float,
    distance_matrix: pd.DataFrame | None,
    n_trucks: int,
    truck_capacity: int,
) -> dict[str, Any]:
    """Build the OR-Tools PDP model dict.

    Layout: ``[depot, pickup_1, delivery_1, pickup_2, delivery_2, ...]``.
    Distances come from ``resolved.distance_matrix`` when available, with a
    Haversine fallback in metres.

    Parameters
    ----------
    pairs
        Pickup-delivery pair dicts from the planner.
    depot_id
        Facility id of the depot node.
    depot_lat
        Depot latitude in degrees.
    depot_lon
        Depot longitude in degrees.
    distance_matrix
        Optional distance matrix DataFrame in km.
    n_trucks
        Number of available trucks (vehicles in the model).
    truck_capacity
        Per-truck capacity in commodity units.

    Returns
    -------
    dict
        Model dict consumable by :func:`_solve_pdp`.
    """
    locations: list[tuple[float, float]] = [(depot_lat, depot_lon)]
    graph_node_ids: list[str | None] = [depot_id]
    node_ids: list[str] = ["depot"]
    demands: list[int] = [0]
    pickups_deliveries: list[tuple[int, int]] = []

    for pair in pairs:
        pickup_idx = len(locations)
        locations.append((pair["pickup_latitude"], pair["pickup_longitude"]))
        graph_node_ids.append(pair["pickup_node_id"])
        node_ids.append(f"{pair['pickup_node_id']}_pickup")
        demands.append(pair["quantity"])

        delivery_idx = len(locations)
        locations.append((pair["delivery_latitude"], pair["delivery_longitude"]))
        graph_node_ids.append(pair["delivery_node_id"])
        node_ids.append(f"{pair['delivery_node_id']}_delivery")
        demands.append(-pair["quantity"])

        pickups_deliveries.append((pickup_idx, delivery_idx))

    edge_distances = build_edge_distance_map(distance_matrix)
    matrix = build_node_distance_matrix(locations, graph_node_ids, edge_distances)

    return {
        "distance_matrix": matrix,
        "demands": demands,
        "pickups_deliveries": pickups_deliveries,
        "resource_capacities": [int(truck_capacity)] * n_trucks,
        "num_resources": int(n_trucks),
        "depot": 0,
        "node_ids": node_ids,
        "graph_node_ids": graph_node_ids,
    }



def _solve_pdp(
    data: dict[str, Any],
    *,
    time_limit_seconds: int,
    random_seed: int,
) -> dict[str, Any] | None:
    """Solve the PDP model with OR-Tools.

    Salvaged from ``gbp/rebalancer/routing/vrp.py:solve_pdp`` plus
    ``gbp/rebalancer/routing/postprocessing.py:extract_pdp_solution``,
    inlined and parameterized with a deterministic seed.

    Parameters
    ----------
    data
        Model dict produced by :func:`_build_pdp_model`.
    time_limit_seconds
        Solver time budget in seconds.
    random_seed
        Deterministic seed for reproducible solutions.

    Returns
    -------
    dict or None
        Solution dict with ``routes``, ``total_distance_m``, and
        ``objective`` keys, or ``None`` when the solver finds no
        feasible solution.
    """
    manager = pywrapcp.RoutingIndexManager(
        len(data["distance_matrix"]),
        data["num_resources"],
        data["depot"],
    )
    routing = pywrapcp.RoutingModel(manager)

    def distance_callback(from_index: int, to_index: int) -> int:
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return int(data["distance_matrix"][from_node][to_node])

    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    def demand_callback(from_index: int) -> int:
        from_node = manager.IndexToNode(from_index)
        return int(data["demands"][from_node])

    demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(
        demand_callback_index,
        0,
        data["resource_capacities"],
        True,
        "Capacity",
    )

    for pickup_idx, delivery_idx in data["pickups_deliveries"]:
        pickup_index = manager.NodeToIndex(pickup_idx)
        delivery_index = manager.NodeToIndex(delivery_idx)
        routing.AddPickupAndDelivery(pickup_index, delivery_index)
        routing.solver().Add(
            routing.VehicleVar(pickup_index) == routing.VehicleVar(delivery_index)
        )
        capacity_dimension = routing.GetDimensionOrDie("Capacity")
        routing.solver().Add(
            capacity_dimension.CumulVar(pickup_index)
            <= capacity_dimension.CumulVar(delivery_index)
        )

    penalty = 100_000
    for node in range(1, len(data["distance_matrix"])):
        routing.AddDisjunction([manager.NodeToIndex(node)], penalty)

    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PARALLEL_CHEAPEST_INSERTION
    )
    search_parameters.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )
    search_parameters.time_limit.seconds = int(time_limit_seconds)
    # Determinism for baseline-vs-treatment comparison.
    try:
        search_parameters.random_seed = int(random_seed)
    except AttributeError:  # pragma: no cover — older OR-Tools
        pass

    solution = routing.SolveWithParameters(search_parameters)
    if solution is None:
        return None

    return _extract_pdp_solution(data, manager, routing, solution)


def _extract_pdp_solution(
    data: dict[str, Any],
    manager: Any,
    routing: Any,
    solution: Any,
) -> dict[str, Any]:
    """Extract truck routes from the OR-Tools solution.

    Salvaged from ``gbp/rebalancer/routing/postprocessing.py``.

    Parameters
    ----------
    data
        Model dict produced by :func:`_build_pdp_model`.
    manager
        OR-Tools ``RoutingIndexManager``.
    routing
        OR-Tools ``RoutingModel``.
    solution
        OR-Tools solution object.

    Returns
    -------
    dict
        Dict with ``routes`` (list of per-truck route dicts),
        ``total_distance_m`` (int), and ``objective`` (int).
    """
    routes: list[dict[str, Any]] = []
    total_distance = 0
    n = len(data["distance_matrix"])

    for resource_id in range(data["num_resources"]):
        route: list[dict[str, Any]] = []
        route_distance = 0
        route_load = 0
        index = routing.Start(resource_id)
        while not routing.IsEnd(index):
            node = manager.IndexToNode(index)
            demand = int(data["demands"][node])
            route_load += demand
            route.append({
                "node_id": data["node_ids"][node],
                "node_index": int(node),
                "demand": demand,
                "cumulative_load": route_load,
            })
            previous_index = index
            index = solution.Value(routing.NextVar(index))
            route_distance += routing.GetArcCostForVehicle(
                previous_index, index, resource_id,
            )
        route.append({
            "node_id": "depot",
            "node_index": 0,
            "demand": 0,
            "cumulative_load": route_load,
        })
        if len(route) > 2:
            # Convert metres to kilometres (matrix is in metres).
            distance_km = route_distance / 1000.0
            routes.append({
                "resource_id": int(resource_id),
                "route": route,
                "distance": float(distance_km),
            })
            total_distance += int(route_distance)

    _ = n  # kept to mirror salvage source; not needed downstream
    return {
        "routes": routes,
        "total_distance_m": int(total_distance),
        "objective": int(solution.ObjectiveValue()),
    }


def _route_to_pairs(
    route_steps: list[dict[str, Any]],
    pairs: list[dict[str, Any]],
) -> list[tuple[str, str, int]]:
    """Walk a route and extract visited pickup-delivery triples.

    Pairs are emitted in pickup-step order, which preserves the chain
    rooted at the depot.  The truck starts at the depot, picks up at
    pickup_i, and delivers at delivery_i; one DataFrame row per i.

    Parameters
    ----------
    route_steps
        Ordered list of route-step dicts from the solver solution.
    pairs
        Original pickup-delivery pair dicts from the planner.

    Returns
    -------
    list of (str, str, int)
        ``(pickup_id, delivery_id, quantity)`` for each pair the truck
        actually visits.
    """
    pickup_qty: dict[str, int] = {}
    out: list[tuple[str, str, int]] = []
    for step in route_steps:
        raw_id = str(step["node_id"])
        demand = int(step["demand"])
        if raw_id == "depot":
            continue
        if "_pickup" in raw_id:
            node_id = raw_id.replace("_pickup", "")
            pickup_qty[node_id] = abs(demand)
        elif "_delivery" in raw_id:
            node_id = raw_id.replace("_delivery", "")
            qty = abs(demand)
            # find the matching pickup for this delivery
            pair = _find_pair_by_delivery(pairs, node_id, qty, pickup_qty)
            if pair is None:
                continue
            out.append((pair[0], pair[1], pair[2]))
    return out


def _find_pair_by_delivery(
    pairs: list[dict[str, Any]],
    delivery_id: str,
    qty: int,
    pickup_qty: dict[str, int],
) -> tuple[str, str, int] | None:
    """Resolve a delivery step to a pickup-delivery triple.

    Picks the first pair whose delivery matches and whose pickup has been
    visited (and whose recorded pickup_qty equals *qty*).

    Parameters
    ----------
    pairs
        Original pickup-delivery pair dicts from the planner.
    delivery_id
        Node id of the delivery step to resolve.
    qty
        Absolute demand at the delivery step.
    pickup_qty
        Mapping of already-visited pickup node ids to their quantities.

    Returns
    -------
    tuple of (str, str, int) or None
        ``(pickup_id, delivery_id, quantity)`` if a matching pair is
        found, ``None`` otherwise.
    """
    for pair in pairs:
        if str(pair["delivery_node_id"]) != delivery_id:
            continue
        if int(pair["quantity"]) != qty:
            continue
        pickup_id = str(pair["pickup_node_id"])
        if pickup_id not in pickup_qty:
            continue
        return (pickup_id, delivery_id, qty)
    return None


def _assert_resource_pre_assigned(
    dispatches: pd.DataFrame, available_truck_ids: set[str],
) -> None:
    """Verify every emitted ``resource_id`` is non-null and known.

    Pickup-delivery rows are *logical* movements, not physical traversal
    legs.  A row's ``source_id`` is a pickup station and may not equal the
    truck's ``current_facility_id`` — therefore chain-rooted validation
    does not apply.  ``DispatchPhase`` checks resource availability at the
    lifecycle level.  This assertion catches solver bugs that would produce
    a non-existent or null ``resource_id``.

    Parameters
    ----------
    dispatches
        Dispatches DataFrame to validate.
    available_truck_ids
        Set of valid truck resource ids.

    Raises
    ------
    RuntimeError
        When any row has a null or unknown ``resource_id``.
    """
    if dispatches.empty:
        return
    if dispatches["resource_id"].isna().any():
        raise RuntimeError(
            "RebalancerTask emitted a row with null resource_id; "
            "all rebalancer rows must be pre-assigned to an available truck.",
        )
    emitted = set(dispatches["resource_id"].astype(str))
    unknown = emitted - available_truck_ids
    if unknown:
        raise RuntimeError(
            "RebalancerTask emitted rows with unknown resource_id "
            f"{sorted(unknown)!r}; expected one of {sorted(available_truck_ids)!r}.",
        )
