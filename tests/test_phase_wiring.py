"""Direct tests for the ordered phase-wiring seams.

The pure mechanics (``dock_up_to_capacity``, ``plan_overflow_redirect``,
``target_inventory``, the VRP solver) have their own tests; the risky part is
the ordering that stitches them together. These three functions each own one
ordered sequence and take ``(state, inputs, period)``-shaped arguments, so a
test can drive them on a hand-built state without the engine and without
OR-Tools:

- :func:`dock_due_arrivals` -- dock, then read the inventory as it will stand,
  then redirect against that reduced inventory;
- :func:`plan_rebalance` -- target -> imbalance -> solver, with a scripted
  solver injected;
- :func:`apply_rebalance` -- the three ordered rounds of plan execution.
"""

import pandas as pd

from gbp.consumers.simulator.engine import init_state
from gbp.consumers.simulator.phases import dock_due_arrivals
from gbp.consumers.simulator.rebalancing import (
    RebalancingParams,
    apply_rebalance,
    assign_bikes_to_stops,
    plan_rebalance,
)
from gbp.consumers.simulator.state import PeriodRow
from gbp.model import flows as J
from tests import scenarios

CLASSIC = scenarios.CLASSIC


def _period(resolved, period_id: int) -> PeriodRow:
    """Return the ``PeriodRow`` of ``period_id`` from a scenario's period grid."""
    row = resolved.periods_df[resolved.periods_df["period_id"] == period_id].iloc[0]
    return PeriodRow(int(row.period_id), row.start_timestamp, row.end_timestamp)


def _due(targets: list[str], t: int) -> pd.DataFrame:
    """Build an in-transit frame of bikes due to dock at ``t``, aimed at ``targets``."""
    return J.departed_events(
        pd.DataFrame(
            {
                "flow_id": [f"f{i}" for i in range(len(targets))],
                "source_id": "s0",
                "planned_target_id": targets,
                "commodity_category": CLASSIC,
                "start_period": 0,
                "planned_end_period": t,
            }
        )
    )


# ---------------------------------------------------------------------------
# DockArrivals: dock, re-read the inventory, then redirect
# ---------------------------------------------------------------------------
def test_dock_due_arrivals_empty_due_is_an_empty_batch():
    """No bikes due this period means no events."""
    resolved = scenarios.build_resolved([("A", "C", 0, 1), ("B", "C", 0, 1)])
    state = init_state(resolved, _period(resolved, 0))
    assert dock_due_arrivals(_due([], 0), state, resolved, 0).empty


def test_dock_due_arrivals_redirects_the_overflow_against_the_post_dock_inventory():
    """The redirect sees the docks the planned dockings just took.

    Station A holds two docks and starts empty; three bikes are due there. Two
    dock at A, filling it, and the third overflows. The redirect reads the
    inventory as it will stand after those two dockings -- A is now full -- so
    the third bike goes to the nearest free station B, not back to A. The A->B
    pair has no historical trip here, so the short leg docks within the period.
    """
    resolved = scenarios.build_resolved(
        [("A", "C", 0, 1), ("B", "C", 0, 1)],
        capacities={"A": 2, "B": 10},
        initial_inventory={"A": 0, "B": 0, "C": 0},
    )
    state = init_state(resolved, _period(resolved, 0))

    batch = dock_due_arrivals(_due(["A", "A", "A"], 0), state, resolved, 0)

    arrived = batch[batch["event_type"] == "arrived"]
    assert (arrived["realized_target_id"] == "A").sum() == 2
    assert (arrived["realized_target_id"] == "B").sum() == 1
    # Exactly one bike bounced, and it opened its new leg toward B.
    assert (batch["event_type"] == "redirected").sum() == 1
    legs = batch[(batch["event_type"] == "departed") & (batch["move_id"] >= 1)]
    assert legs["planned_target_id"].tolist() == ["B"]


# ---------------------------------------------------------------------------
# PlanRebalancingPhase: target -> imbalance -> scripted solver
# ---------------------------------------------------------------------------
def test_plan_rebalance_builds_the_plan_with_a_scripted_solver():
    """A morning shortage at s2 becomes a plan that trucks bikes s1 -> s2."""
    trips = [("s2", "s1", 6, 7)] * 3
    resolved = scenarios.with_rebalancing_data(
        scenarios.build_resolved(trips, initial_inventory={"s1": 5, "s2": 0})
    )
    state = init_state(resolved, _period(resolved, 1))
    calls: list[pd.DataFrame] = []

    def scripted_solver(nodes, travel_minutes, trucks, params):
        calls.append(nodes)
        return scenarios.scripted_stops(3)

    plan = plan_rebalance(
        state, resolved, _period(resolved, 1), RebalancingParams(), scripted_solver
    )
    assert len(calls) == 1
    assert len(plan) == 3
    assert (plan["source_id"] == "s1").all()
    assert (plan["planned_target_id"] == "s2").all()


def test_plan_rebalance_with_no_shortage_returns_empty_and_skips_the_solver():
    """When no station is short, the plan is empty and the solver is not called."""
    trips = [("s1", "s2", 6, 7)]
    resolved = scenarios.with_rebalancing_data(
        scenarios.build_resolved(trips, initial_inventory={"s1": 5, "s2": 5})
    )
    state = init_state(resolved, _period(resolved, 1))

    def failing_solver(nodes, travel_minutes, trucks, params):
        raise AssertionError("the solver must not be called when nothing is short")

    plan = plan_rebalance(
        state, resolved, _period(resolved, 1), RebalancingParams(), failing_solver
    )
    assert plan.empty


# ---------------------------------------------------------------------------
# ApplyRebalancingPhase: the three ordered rounds
# ---------------------------------------------------------------------------
def test_apply_rebalance_executes_due_pickups_and_shrinks_the_plan():
    """Pickups due this period fire (round 1); the executed rows leave the plan."""
    trips = [("s2", "s1", 6, 7)] * 3
    resolved = scenarios.with_rebalancing_data(
        scenarios.build_resolved(trips, initial_inventory={"s1": 5, "s2": 0})
    )
    plan = assign_bikes_to_stops(
        scenarios.scripted_stops(3), window_period_id=1, minutes_per_period=60
    )
    state = init_state(resolved, _period(resolved, 1)).with_rebalance_plan(plan)

    events, remaining = apply_rebalance(state, resolved, 1)

    departed = events[events["event_type"] == "departed"]
    assert len(departed) == 3
    assert (departed["source_id"] == "s1").all()
    # A pickup carries no dropoff this period (dropoff is period 2), so the whole
    # batch is round 1 -- the pickup round.
    assert (events["phase_round"] == 1).all()
    assert remaining.empty


def test_apply_rebalance_pickup_is_cut_to_the_bikes_on_hand():
    """A source with fewer bikes than the plan asks for executes only what it has."""
    trips = [("s2", "s1", 6, 7)] * 3
    resolved = scenarios.with_rebalancing_data(
        scenarios.build_resolved(trips, initial_inventory={"s1": 2, "s2": 0})
    )
    plan = assign_bikes_to_stops(
        scenarios.scripted_stops(3), window_period_id=1, minutes_per_period=60
    )
    state = init_state(resolved, _period(resolved, 1)).with_rebalance_plan(plan)

    events, remaining = apply_rebalance(state, resolved, 1)

    # s1 holds two bikes, so the third plan row cannot be picked up.
    assert len(events[events["event_type"] == "departed"]) == 2
    assert remaining.empty


def test_apply_rebalance_with_no_plan_and_nothing_in_transit_is_a_noop():
    """An empty plan and no rebalance bike riding means no events and an empty plan."""
    resolved = scenarios.with_rebalancing_data(
        scenarios.build_resolved([("s1", "s2", 0, 1)], initial_inventory={"s1": 5})
    )
    state = init_state(resolved, _period(resolved, 0))

    events, remaining = apply_rebalance(state, resolved, 0)

    assert events.empty
    assert remaining.empty
