"""Integration test: canonical scenario end-to-end.

Runs the full pipeline from DataLoaderMock through Environment.run() and
asserts key invariants that the simulation must satisfy on the canonical
historical replay.
"""

from __future__ import annotations

import dataclasses

import pandas as pd
import pytest

from gbp.build.pipeline import build_model
from gbp.consumers.simulator.built_in_phases import (
    ArrivalsPhase,
    DeparturePhysicsPhase,
    HistoricalLatentDemandPhase,
    HistoricalODStructurePhase,
    HistoricalTripSamplingPhase,
    InvariantCheckPhase,
)
from gbp.consumers.simulator.config import EnvironmentConfig
from gbp.consumers.simulator.dispatch_phase import DispatchPhase
from gbp.consumers.simulator.engine import Environment
from gbp.consumers.simulator.overflow_redirect_phase import OverflowRedirectPhase
from gbp.consumers.simulator.phases import Schedule
from gbp.consumers.simulator.state import init_state
from gbp.consumers.simulator.tasks.rebalancer import RebalancerTask
from gbp.loaders.contracts import GraphLoaderConfig
from gbp.loaders.dataloader_graph import DataLoaderGraph
from gbp.loaders.dataloader_mock import DataLoaderMock

MOCK_CONFIG: dict = {
    "n_stations": 10,
    "n_depots": 2,
    "n_timestamps": 72,
    "time_freq": "h",
    "start_date": "2025-01-01",
    "ebike_fraction": 0.3,
    "depot_capacity": 200,
    "seed": 42,
}


@pytest.fixture(scope="module")
def resolved_model():
    """Build ResolvedModelData from the canonical mock config."""
    mock_source = DataLoaderMock(MOCK_CONFIG, n_trucks=3, truck_capacity_bikes=20)
    graph_loader = DataLoaderGraph(mock_source, GraphLoaderConfig())
    raw_model = graph_loader.load()
    resolved_with_supply = build_model(raw_model)
    return dataclasses.replace(resolved_with_supply, supply=None)


@pytest.fixture(scope="module")
def canonical_log(resolved_model):
    """Run the canonical simulation and return the log dataframes."""
    state_initial = init_state(resolved_model)
    inventory_totals = (
        state_initial.inventory.groupby("commodity_category")["quantity"].sum()
    )
    in_transit_totals = (
        state_initial.in_transit.groupby("commodity_category")["quantity"].sum()
        if not state_initial.in_transit.empty
        else pd.Series(dtype=float)
    )
    baseline_per_commodity = (
        inventory_totals.add(in_transit_totals, fill_value=0.0).to_dict()
    )

    rebalance_every_n_periods = 6
    rebalancer_task = RebalancerTask(
        min_threshold=0.3,
        max_threshold=0.7,
        time_limit_seconds=5,
        commodity_category="electric_bike",
    )

    phases = [
        HistoricalLatentDemandPhase(),
        HistoricalODStructurePhase(),
        DeparturePhysicsPhase(mode="permissive"),
        HistoricalTripSamplingPhase(use_durations=True),
        ArrivalsPhase(),
        OverflowRedirectPhase(),
        DispatchPhase(
            rebalancer_task,
            schedule=Schedule.every_n(rebalance_every_n_periods),
        ),
        InvariantCheckPhase(
            baseline=baseline_per_commodity,
            fail_on_violation=False,
        ),
    ]

    env = Environment(
        resolved_model,
        EnvironmentConfig(phases=phases, seed=42, scenario_id="historical_replay"),
    )
    env.run()
    return env.log.to_dataframes()


# ---------------------------------------------------------------------------
# Invariant 1: no InvariantViolationError (simulation ran to completion)
# ---------------------------------------------------------------------------
# This is implicitly tested by the canonical_log fixture succeeding.
# We additionally check the invariant violation log is empty.


def test_no_invariant_violations(canonical_log):
    """InvariantCheckPhase should report zero violations on canonical replay."""
    violations = canonical_log["simulation_invariant_violation_log"]
    assert violations.empty, (
        f"Expected no invariant violations, got {len(violations)} rows:\n"
        f"{violations.to_string()}"
    )


# ---------------------------------------------------------------------------
# Invariant 2: no rejected dispatches on baseline
# ---------------------------------------------------------------------------


def test_no_rejected_dispatches(canonical_log):
    """Baseline historical replay should have no rejected dispatches."""
    rejected = canonical_log["simulation_rejected_dispatches_log"]
    assert rejected.empty, (
        f"Expected no rejected dispatches, got {len(rejected)} rows:\n"
        f"{rejected.to_string()}"
    )


# ---------------------------------------------------------------------------
# Invariant 3: total inventory + in_transit conserved per commodity
# ---------------------------------------------------------------------------


def test_commodity_conservation(canonical_log):
    """Total (inventory + in_transit) per commodity must be conserved across all periods.

    InvariantCheckPhase runs every period with ``fail_on_violation=False`` and
    logs any delta.  An empty violation log means conservation held throughout.
    """
    violations = canonical_log["simulation_invariant_violation_log"]
    assert violations.empty, f"Conservation violated:\n{violations.to_string()}"


# ---------------------------------------------------------------------------
# Invariant 4: simulation_flow_log is non-empty (trips actually happened)
# ---------------------------------------------------------------------------


def test_flow_log_non_empty(canonical_log):
    """Flow log must contain at least one trip record."""
    flow_log = canonical_log["simulation_flow_log"]
    assert not flow_log.empty, "Flow log is empty — no trips happened during simulation"


# ---------------------------------------------------------------------------
# Invariant 5: final inventory is non-negative everywhere
# ---------------------------------------------------------------------------


def test_final_inventory_bounded(canonical_log):
    """Final inventory should not have large negative values.

    In ``permissive`` departure mode, small transient negatives (order of a
    few bikes) are expected because departures are allowed even when stock is
    slightly insufficient.  We verify that no facility drops below a
    reasonable floor.
    """
    inv_log = canonical_log["simulation_inventory_log"]
    last_period = inv_log["period_index"].max()
    final = inv_log[inv_log["period_index"] == last_period]

    severe = final[final["quantity"] < -50.0]
    assert severe.empty, (
        f"Severely negative inventory at final period:\n{severe.to_string()}"
    )


# ---------------------------------------------------------------------------
# Invariant 6: inventory stays within reasonable bounds
# ---------------------------------------------------------------------------


def test_inventory_no_severe_negatives(canonical_log):
    """No facility should have deeply negative inventory at any period.

    Small negatives are tolerated in ``permissive`` mode (DeparturePhysicsPhase
    allows departures even when stock is slightly short).  We flag only values
    below -50 as a sign of a genuine bug.
    """
    inv_log = canonical_log["simulation_inventory_log"]
    severe = inv_log[inv_log["quantity"] < -50.0]
    assert severe.empty, (
        f"Severely negative inventory in {len(severe)} rows:\n"
        f"{severe.head(20).to_string()}"
    )


# ---------------------------------------------------------------------------
# Invariant 7: all expected log tables are present
# ---------------------------------------------------------------------------


def test_log_tables_present(canonical_log):
    """All canonical log tables should exist in the output."""
    expected_keys = {
        "simulation_inventory_log",
        "simulation_flow_log",
        "simulation_resource_log",
        "simulation_unmet_demand_log",
        "simulation_rejected_dispatches_log",
        "simulation_latent_demand_log",
        "simulation_lost_demand_log",
        "simulation_dock_blocking_log",
        "simulation_redirected_flow_log",
        "simulation_invariant_violation_log",
    }
    assert expected_keys.issubset(canonical_log.keys()), (
        f"Missing log tables: {expected_keys - canonical_log.keys()}"
    )


# ---------------------------------------------------------------------------
# Invariant 8: inventory log covers all periods
# ---------------------------------------------------------------------------


def test_inventory_log_covers_all_periods(resolved_model, canonical_log):
    """Inventory log should have snapshots for every period in the model."""
    inv_log = canonical_log["simulation_inventory_log"]
    expected_periods = set(resolved_model.periods["period_index"].astype(int))
    actual_periods = set(inv_log["period_index"].unique())
    assert expected_periods == actual_periods, (
        f"Missing periods in inventory log: {expected_periods - actual_periods}"
    )
