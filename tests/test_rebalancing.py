"""Rebalancing tests: planning math, the solver, plan execution, and full runs.

Most full-run stories inject a small hand-written solver stand-in returning a
fixed stops table, so the phases and the plan bookkeeping are exercised
deterministically no matter what the routing search does. The real OR-Tools
core (``solve_rebalance_vrp``) has its own tests, plus one full run routed by
it end to end.

The full-run stories reuse the synthetic scenario builders in
:mod:`tests.scenarios`: :func:`tests.scenarios.build_resolved` plus
:func:`tests.scenarios.with_rebalancing_data`, which adds the tables the
rebalancing phases read (a depot, the truck fleet, the historical arrivals
and the period length).
"""

import numpy as np
import pandas as pd
import pytest

from gbp.consumers.simulator import canonical_phases, rebalancing_phases
from gbp.consumers.simulator.rebalancing import (
    RebalancingParams,
    assign_bikes_to_stops,
    build_rebalance_nodes,
    clip_dropoffs_to_free_docks,
    solve_rebalance_vrp,
    station_imbalance,
    target_inventory,
)
from gbp.consumers.simulator.validation import validate_run
from gbp.loaders.dataloader_graph import apply_truck_fleet
from gbp.model import flows as J
from tests import scenarios
from tests.invariants import check_journal_well_formed

CLASSIC = scenarios.CLASSIC
DEPOT = scenarios.DEPOT
TRUCK = scenarios.TRUCK


# ---------------------------------------------------------------------------
# Predicates: the -1 side of the inventory rule vs the demand side
# ---------------------------------------------------------------------------
def test_is_undocking_vs_is_user_departure():
    """A rebalance pickup undocks a bike but is not a user departure."""
    flows = pd.DataFrame(
        {
            "event_type": ["departed", "departed", "departed", "arrived"],
            "move_id": [0, 1, 0, 0],
            "flow_type": ["user_trip", "user_trip", "rebalance", "rebalance"],
        }
    )
    assert J.is_undocking(flows).tolist() == [True, False, True, False]
    assert J.is_user_departure(flows).tolist() == [True, False, False, False]


# ---------------------------------------------------------------------------
# Planning math
# ---------------------------------------------------------------------------
def _morning_grid() -> pd.DataFrame:
    """Build a period grid whose periods 6 and 7 start at 06:00 and 07:00."""
    periods = pd.DataFrame({"period_id": range(8)})
    periods["start_timestamp"] = pd.Timestamp("2026-01-01") + periods["period_id"] * pd.Timedelta(
        hours=1
    )
    periods["end_timestamp"] = periods["start_timestamp"] + pd.Timedelta(hours=1)
    return periods


def test_target_inventory_is_the_peak_of_running_departures_minus_arrivals():
    """Target = highest running total of departures minus arrivals over the morning."""
    demand = pd.DataFrame(
        {
            "period_id": [6, 7, 6],
            "facility_id": ["s1", "s1", "s2"],
            "commodity_category": CLASSIC,
            "quantity": [4, 2, 0],
        }
    )
    arrivals = pd.DataFrame(
        {
            "period_id": [6, 6],
            "facility_id": ["s1", "s2"],
            "commodity_category": CLASSIC,
            "quantity": [1, 5],
        }
    )
    target = target_inventory(
        demand,
        arrivals,
        _morning_grid(),
        pd.Timestamp("2026-01-01 01:00"),
        RebalancingParams(),
    )
    # s1: net +3 then +2, running total peaks at 5. s2 only receives: no row.
    assert target.set_index("facility_id")["target"].to_dict() == {"s1": 5}


def test_target_inventory_scales_with_demand():
    """The demand scale factor multiplies both departures and arrivals."""
    demand = pd.DataFrame(
        {
            "period_id": [6],
            "facility_id": ["s1"],
            "commodity_category": CLASSIC,
            "quantity": [4],
        }
    )
    arrivals = pd.DataFrame(
        {
            "period_id": [6],
            "facility_id": ["s1"],
            "commodity_category": CLASSIC,
            "quantity": [1],
        }
    )
    target = target_inventory(
        demand,
        arrivals,
        _morning_grid(),
        pd.Timestamp("2026-01-01 01:00"),
        RebalancingParams(),
        demand_scale_factor=2.0,
    )
    assert target["target"].tolist() == [6]  # (4 - 1) * 2


def test_station_imbalance_signs():
    """Positive = bikes to give, negative = bikes needed; missing rows count as 0."""
    inventory = pd.DataFrame(
        {
            "facility_id": ["s1", "s2"],
            "commodity_category": CLASSIC,
            "quantity": [5, 0],
        }
    )
    target = pd.DataFrame(
        {"facility_id": ["s2", "s3"], "commodity_category": CLASSIC, "target": [3, 2]}
    )
    out = station_imbalance(inventory, target).set_index("facility_id")["imbalance"]
    assert out.to_dict() == {"s1": 5, "s2": -3, "s3": -2}


def test_clip_dropoffs_to_free_docks():
    """A station's planned inflow is cut down to its free dock slots."""
    imbalance = pd.DataFrame(
        {
            "facility_id": ["s1", "s2"],
            "commodity_category": CLASSIC,
            "imbalance": [5, -8],
        }
    )
    inventory = pd.DataFrame(
        {"facility_id": ["s1", "s2"], "commodity_category": CLASSIC, "quantity": [5, 1]}
    )
    capacities = pd.DataFrame({"facility_id": ["s1", "s2"], "capacity": [10, 4]})
    out = clip_dropoffs_to_free_docks(imbalance, inventory, capacities)
    out = out.set_index("facility_id")["imbalance"]
    assert out["s1"] == 5  # pickups untouched
    assert out["s2"] == -3  # 4 docks - 1 bike on hand = 3 free slots


def test_build_rebalance_nodes_matches_totals_and_splits_into_portions():
    """Only min(surplus, shortage) bikes move; the shares split into portions.

    s1 has 12 bikes over its target but s2 is short only 7, so 7 move: the
    pickup side is trimmed to 7 and both sides split into portions of 5 and 2.
    """
    imbalance = pd.DataFrame(
        {
            "facility_id": ["s1", "s2", "s3"],
            "commodity_category": CLASSIC,
            "imbalance": [12, -7, 0],
        }
    )
    nodes = build_rebalance_nodes(imbalance, portion_size=5)
    pickups = nodes[nodes["node_type"] == "pickup"]
    dropoffs = nodes[nodes["node_type"] == "dropoff"]
    assert pickups["quantity"].tolist() == [5, 2]
    assert (pickups["facility_id"] == "s1").all()
    assert dropoffs["quantity"].tolist() == [5, 2]
    assert "s3" not in set(nodes["facility_id"])


def test_build_rebalance_nodes_trims_the_smallest_imbalances_first():
    """When trimming, facilities with the largest imbalance keep their share."""
    imbalance = pd.DataFrame(
        {
            "facility_id": ["s1", "s2", "s3"],
            "commodity_category": CLASSIC,
            "imbalance": [5, 4, -6],
        }
    )
    nodes = build_rebalance_nodes(imbalance, portion_size=5)
    pickups = nodes[nodes["node_type"] == "pickup"]
    # 6 bikes move: s1 (larger surplus) keeps all 5, s2 keeps only 1.
    kept = pickups.groupby("facility_id")["quantity"].sum().to_dict()
    assert kept == {"s1": 5, "s2": 1}
    assert nodes[nodes["node_type"] == "dropoff"]["quantity"].sum() == 6


def test_assign_bikes_to_stops_matches_earliest_pickups_and_buckets_minutes():
    """A dropoff hands over the bikes picked up earliest; minutes become periods."""
    stops = pd.DataFrame(
        [
            {
                "resource_id": TRUCK,
                "stop_seq": 0,
                "facility_id": "s1",
                "stop_type": "pickup",
                "commodity_category": CLASSIC,
                "quantity": 2,
                "minute": 10.0,
            },
            {
                "resource_id": TRUCK,
                "stop_seq": 1,
                "facility_id": "s2",
                "stop_type": "pickup",
                "commodity_category": CLASSIC,
                "quantity": 1,
                "minute": 30.0,
            },
            {
                "resource_id": TRUCK,
                "stop_seq": 2,
                "facility_id": "s3",
                "stop_type": "dropoff",
                "commodity_category": CLASSIC,
                "quantity": 2,
                "minute": 50.0,
            },
            {
                "resource_id": TRUCK,
                "stop_seq": 3,
                "facility_id": "s4",
                "stop_type": "dropoff",
                "commodity_category": CLASSIC,
                "quantity": 1,
                "minute": 70.0,
            },
        ]
    )
    plan = assign_bikes_to_stops(stops, window_period_id=1, minutes_per_period=60)
    assert len(plan) == 3
    # The two bikes picked up first (at s1) go to the first dropoff (s3).
    assert plan["source_id"].tolist() == ["s1", "s1", "s2"]
    assert plan["planned_target_id"].tolist() == ["s3", "s3", "s4"]
    # Minute 10/30/50 fall in the window period (1); minute 70 in the next (2).
    assert plan["pickup_period"].tolist() == [1, 1, 1]
    assert plan["dropoff_period"].tolist() == [1, 1, 2]
    assert plan["flow_id"].is_unique


# ---------------------------------------------------------------------------
# Full runs with a scripted solver
# ---------------------------------------------------------------------------
def test_rebalancing_moves_bikes_and_serves_the_morning_demand():
    """Three bikes trucked s1 -> s2 overnight turn 3 stockouts into 3 departures.

    Story: s2 faces 3 trips at 06:00 but starts the day empty; s1 holds 5
    bikes nobody asks for. The scripted route picks 3 bikes at s1 within the
    01:00 period and drops them at s2 within the 02:00 period (minute 70 --
    the route crosses the period edge without returning to the depot).
    """
    trips = [("s2", "s1", 6, 7)] * 3
    resolved = scenarios.with_rebalancing_data(
        scenarios.build_resolved(trips, initial_inventory={"s1": 5, "s2": 0})
    )
    calls: list[pd.DataFrame] = []

    def scripted_solver(nodes, travel_minutes, trucks, params):
        calls.append(nodes)
        assert trucks["home_facility_id"].tolist() == [DEPOT]
        assert DEPOT in travel_minutes.index
        return scenarios.scripted_stops(3)

    journal, state = scenarios.run(
        resolved,
        phases=canonical_phases() + rebalancing_phases(RebalancingParams(), scripted_solver),
    )

    # The planner fired exactly once: only period 1 starts at 01:00.
    assert len(calls) == 1
    nodes = calls[0]
    assert set(nodes["node_type"]) == {"pickup", "dropoff"}
    assert nodes.loc[nodes["node_type"] == "pickup", "facility_id"].tolist() == ["s1"]
    assert nodes.loc[nodes["node_type"] == "dropoff", "quantity"].sum() == 3

    rebalance = journal[journal["flow_type"] == "rebalance"]
    pickups = rebalance[rebalance["event_type"] == "departed"]
    dropoffs = rebalance[rebalance["event_type"] == "arrived"]
    assert len(pickups) == 3 and (pickups["period_id"] == 1).all()
    assert (pickups["source_id"] == "s1").all()
    assert (pickups["resource_id"] == TRUCK).all()
    assert len(dropoffs) == 3 and (dropoffs["period_id"] == 2).all()
    assert (dropoffs["realized_target_id"] == "s2").all()
    assert (rebalance["phase_rank"] == J.REBALANCE_RANK).all()

    # The morning demand at s2 is now served: 3 departures, no stockout.
    morning = journal[journal["period_id"] == 6]
    served = morning[J.is_user_departure(morning) & (morning["source_id"] == "s2")]
    lost = morning[(morning["event_type"] == "lost") & (morning["reason"] == "stockout")]
    assert len(served) == 3
    assert lost.empty

    assert state.rebalance_plan.empty
    assert check_journal_well_formed(journal) == []
    assert validate_run(state, resolved) == []


def test_without_rebalancing_the_same_story_loses_the_morning_demand():
    """The baseline of the story above: s2's 3 trips are stockouts."""
    trips = [("s2", "s1", 6, 7)] * 3
    resolved = scenarios.with_rebalancing_data(
        scenarios.build_resolved(trips, initial_inventory={"s1": 5, "s2": 0})
    )
    journal, _ = scenarios.run(resolved, phases=canonical_phases())
    lost = journal[(journal["event_type"] == "lost") & (journal["reason"] == "stockout")]
    assert int(lost["quantity"].sum()) == 3


def test_pickups_are_cut_to_the_bikes_on_hand():
    """Night demand empties part of the source; the pickup shrinks, the run stays valid.

    The plan asks for 3 bikes at s1, but 3 night riders leave s1 within the
    01:00 period before the truck arrives, so only 2 bikes remain. The pickup
    executes for 2; the third plan row is dropped together with its dropoff.
    """
    trips = [("s1", "s2", 1, 2)] * 3 + [("s2", "s1", 6, 7)] * 3
    resolved = scenarios.with_rebalancing_data(
        scenarios.build_resolved(trips, initial_inventory={"s1": 5, "s2": 0})
    )

    def scripted_solver(nodes, travel_minutes, trucks, params):
        return scenarios.scripted_stops(3)

    journal, state = scenarios.run(
        resolved,
        phases=canonical_phases() + rebalancing_phases(RebalancingParams(), scripted_solver),
    )

    rebalance = journal[journal["flow_type"] == "rebalance"]
    assert len(rebalance[rebalance["event_type"] == "departed"]) == 2
    assert len(rebalance[rebalance["event_type"] == "arrived"]) == 2
    assert check_journal_well_formed(journal) == []
    assert validate_run(state, resolved) == []


def test_dropoff_overflow_docks_at_the_depot():
    """Bikes that find the station full dock at the truck's home depot.

    s2 has 2 docks and the truck brings 3 bikes: 2 dock at s2, the third
    docks at the depot; its ``planned_target_id`` stays s2, so the plan-vs-
    reality difference is visible in the journal.
    """
    trips = [("s2", "s1", 6, 7)] * 3
    resolved = scenarios.with_rebalancing_data(
        scenarios.build_resolved(trips, capacities={"s2": 2}, initial_inventory={"s1": 5, "s2": 0})
    )

    def scripted_solver(nodes, travel_minutes, trucks, params):
        return scenarios.scripted_stops(3)

    journal, state = scenarios.run(
        resolved,
        phases=canonical_phases() + rebalancing_phases(RebalancingParams(), scripted_solver),
    )

    dropoffs = journal[(journal["flow_type"] == "rebalance") & (journal["event_type"] == "arrived")]
    assert dropoffs["realized_target_id"].value_counts().to_dict() == {"s2": 2, DEPOT: 1}
    assert (dropoffs["planned_target_id"] == "s2").all()
    assert check_journal_well_formed(journal) == []
    assert validate_run(state, resolved) == []


def test_no_shortage_means_no_plan_and_no_solver_call():
    """When no station is short of bikes, the planner stores an empty plan."""
    trips = [("s1", "s2", 6, 7)]
    resolved = scenarios.with_rebalancing_data(
        scenarios.build_resolved(trips, initial_inventory={"s1": 5, "s2": 5})
    )

    def failing_solver(nodes, travel_minutes, trucks, params):
        raise AssertionError("the solver must not be called when nothing is short")

    journal, state = scenarios.run(
        resolved,
        phases=canonical_phases() + rebalancing_phases(RebalancingParams(), failing_solver),
    )
    assert journal[journal["flow_type"] == "rebalance"].empty
    assert state.rebalance_plan.empty
    assert validate_run(state, resolved) == []


# ---------------------------------------------------------------------------
# The OR-Tools solver
# ---------------------------------------------------------------------------
FAST = RebalancingParams(solver_time_limit_seconds=1)


def _uniform_travel(facilities: list[str], minutes: float = 5.0) -> pd.DataFrame:
    """Build a travel matrix with the same minutes between any two facilities."""
    grid = np.full((len(facilities), len(facilities)), minutes)
    np.fill_diagonal(grid, 0.0)
    return pd.DataFrame(grid, index=facilities, columns=facilities)


def _nodes(rows: list[tuple[str, str, str, int]]) -> pd.DataFrame:
    """Build a nodes table from (facility, commodity, node_type, quantity) tuples."""
    return pd.DataFrame(
        rows, columns=["facility_id", "commodity_category", "node_type", "quantity"]
    )


def _one_truck(capacity: int) -> pd.DataFrame:
    return pd.DataFrame(
        {"resource_id": [TRUCK], "capacity": [capacity], "home_facility_id": [DEPOT]}
    )


def test_solver_serves_balanced_nodes_within_the_window():
    """One pickup and one matching dropoff: the route is depot -> s1 -> s2 -> depot."""
    nodes = _nodes([("s1", CLASSIC, "pickup", 3), ("s2", CLASSIC, "dropoff", 3)])
    stops = solve_rebalance_vrp(nodes, _uniform_travel([DEPOT, "s1", "s2"]), _one_truck(20), FAST)
    assert stops["stop_type"].tolist() == ["pickup", "dropoff"]
    assert stops["facility_id"].tolist() == ["s1", "s2"]
    assert stops["stop_seq"].tolist() == [0, 1]
    # Arrival minutes: 5 to reach s1; then 3.5 of service (2 + 0.5 * 3) plus 5.
    assert stops["minute"].tolist() == pytest.approx([5.0, 13.5])

    plan = assign_bikes_to_stops(stops, window_period_id=1, minutes_per_period=60)
    assert len(plan) == 3
    assert (plan["source_id"] == "s1").all()
    assert (plan["planned_target_id"] == "s2").all()


def test_solver_skips_nodes_larger_than_the_truck():
    """A 3-bike portion cannot ride a 2-bike truck; both nodes are skipped."""
    nodes = _nodes([("s1", CLASSIC, "pickup", 3), ("s2", CLASSIC, "dropoff", 3)])
    stops = solve_rebalance_vrp(nodes, _uniform_travel([DEPOT, "s1", "s2"]), _one_truck(2), FAST)
    assert stops.empty


def test_solver_skips_nodes_that_do_not_fit_the_window():
    """Stations 200 minutes away cannot be visited inside a 120-minute window."""
    nodes = _nodes([("s1", CLASSIC, "pickup", 3), ("s2", CLASSIC, "dropoff", 3)])
    stops = solve_rebalance_vrp(
        nodes, _uniform_travel([DEPOT, "s1", "s2"], minutes=200.0), _one_truck(20), FAST
    )
    assert stops.empty


def test_each_truck_starts_from_its_own_home_depot():
    """Two trucks, two depots: only the truck whose depot is near can serve.

    ``depot_2`` sits 200 minutes from every other facility, so ``truck_2``
    cannot reach any node and return within the 120-minute window. All the
    work falls to ``truck_1``, based at the near ``depot_1``.
    """
    far_depot = "depot_2"
    nodes = _nodes([("s1", CLASSIC, "pickup", 3), ("s2", CLASSIC, "dropoff", 3)])
    travel = _uniform_travel([DEPOT, far_depot, "s1", "s2"])
    travel.loc[far_depot, :] = 200.0
    travel.loc[:, far_depot] = 200.0
    travel.loc[far_depot, far_depot] = 0.0
    trucks = pd.DataFrame(
        {
            "resource_id": ["truck_1", "truck_2"],
            "capacity": [20, 20],
            "home_facility_id": [DEPOT, far_depot],
        }
    )
    stops = solve_rebalance_vrp(nodes, travel, trucks, FAST)
    assert len(stops) == 2
    assert (stops["resource_id"] == "truck_1").all()
    assert stops["facility_id"].tolist() == ["s1", "s2"]


def test_solver_keeps_commodities_apart():
    """Bikes of each commodity go to a dropoff of the same commodity."""
    electric = "electric_bike"
    nodes = _nodes(
        [
            ("s1", CLASSIC, "pickup", 2),
            ("s1", electric, "pickup", 1),
            ("s2", CLASSIC, "dropoff", 2),
            ("s3", electric, "dropoff", 1),
        ]
    )
    stops = solve_rebalance_vrp(
        nodes, _uniform_travel([DEPOT, "s1", "s2", "s3"]), _one_truck(20), FAST
    )
    assert len(stops) == 4  # every node is served

    plan = assign_bikes_to_stops(stops, window_period_id=1, minutes_per_period=60)
    by_commodity = plan.groupby("commodity_category")["planned_target_id"].unique()
    assert by_commodity[CLASSIC].tolist() == ["s2"]
    assert by_commodity[electric].tolist() == ["s3"]


def test_full_run_with_the_real_solver():
    """The story of the first full-run test, now routed by OR-Tools itself."""
    trips = [("s2", "s1", 6, 7)] * 3
    resolved = scenarios.with_rebalancing_data(
        scenarios.build_resolved(trips, initial_inventory={"s1": 5, "s2": 0})
    )
    journal, state = scenarios.run(resolved, phases=canonical_phases() + rebalancing_phases(FAST))

    rebalance = journal[journal["flow_type"] == "rebalance"]
    assert len(rebalance[rebalance["event_type"] == "departed"]) == 3
    dropoffs = rebalance[rebalance["event_type"] == "arrived"]
    assert (dropoffs["realized_target_id"] == "s2").all()

    morning = journal[journal["period_id"] == 6]
    served = morning[J.is_user_departure(morning) & (morning["source_id"] == "s2")]
    lost = morning[(morning["event_type"] == "lost") & (morning["reason"] == "stockout")]
    assert len(served) == 3
    assert lost.empty
    assert check_journal_well_formed(journal) == []
    assert validate_run(state, resolved) == []


# ---------------------------------------------------------------------------
# The truck fleet as a run parameter
# ---------------------------------------------------------------------------
def test_apply_truck_fleet_rebuilds_the_resource_tables():
    """One home entry per truck; the three resource tables are rebuilt to match."""
    resolved = scenarios.with_rebalancing_data(scenarios.build_resolved([("s1", "s2", 0, 1)]))
    out = apply_truck_fleet(
        resolved, ["depot_1", "depot_1"], truck_capacity_bikes=15, truck_rate=50.0
    )
    assert out.resources_df["resource_id"].tolist() == ["truck_1", "truck_2"]
    assert (out.resources_df["home_facility_id"] == "depot_1").all()
    assert out.resources_capacities_df["capacity"].tolist() == [15, 15]
    assert out.resources_rates_df["rate"].tolist() == [50.0, 50.0]
    # The input container keeps its own fleet (the copy is shallow).
    assert resolved.resources_df["resource_id"].tolist() == [TRUCK]


def test_apply_truck_fleet_rejects_bad_homes():
    """An empty fleet or a home that is not a depot facility is a ValueError."""
    resolved = scenarios.with_rebalancing_data(scenarios.build_resolved([("s1", "s2", 0, 1)]))
    with pytest.raises(ValueError, match="at least one truck"):
        apply_truck_fleet(resolved, [], truck_capacity_bikes=15, truck_rate=50.0)
    with pytest.raises(ValueError, match="not depot facilities"):
        # s1 is a station, not a depot.
        apply_truck_fleet(resolved, ["s1"], truck_capacity_bikes=15, truck_rate=50.0)
