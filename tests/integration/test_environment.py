"""Integration tests for the Environment simulation engine.

Tests the full pipeline: make_raw_model -> build_model -> Environment.run().
"""
# ruff: noqa: D102

from __future__ import annotations

import dataclasses

import pandas as pd

from gbp.build.pipeline import build_model
from gbp.consumers.simulator.config import EnvironmentConfig
from gbp.consumers.simulator.dispatch_phase import DispatchPhase
from gbp.consumers.simulator.engine import Environment
from gbp.consumers.simulator.tasks.noop import NoopTask
from tests.unit.build.fixtures import minimal_raw_model


def _build_resolved_with_inventory():
    """Build a ResolvedModelData with inventory_initial from the minimal fixture."""
    raw = minimal_raw_model(with_demand=True, with_supply=False)
    raw = dataclasses.replace(
        raw,
        inventory_initial=pd.DataFrame(
            {
                "facility_id": ["d1", "s1", "s2"],
                "commodity_category": ["working_bike"] * 3,
                "quantity": [50.0, 12.0, 7.0],
            }
        ),
    )
    return build_model(raw)


class TestEnvironmentEmptyPhasesIntegration:
    """Full pipeline with empty phases — verifies engine + log wiring."""

    def test_run_produces_all_log_tables(self) -> None:
        resolved = _build_resolved_with_inventory()
        config = EnvironmentConfig(phases=[])
        env = Environment(resolved, config)

        log = env.run()
        dfs = log.to_dataframes()

        assert set(dfs.keys()) == {
            "simulation_inventory_log",
            "simulation_flow_log",
            "simulation_resource_log",
            "simulation_unmet_demand_log",
            "simulation_rejected_dispatches_log",
            "simulation_latent_demand_log",
            "simulation_lost_demand_log",
            "simulation_dock_blocking_log",
        }

        # Inventory: 3 periods x 3 facilities
        assert len(dfs["simulation_inventory_log"]) == 9

        # Resources: 3 periods x 3 trucks
        assert len(dfs["simulation_resource_log"]) == 9

        # No events without phases
        assert len(dfs["simulation_flow_log"]) == 0
        assert len(dfs["simulation_unmet_demand_log"]) == 0
        assert len(dfs["simulation_rejected_dispatches_log"]) == 0

    def test_inventory_total_constant(self) -> None:
        resolved = _build_resolved_with_inventory()
        config = EnvironmentConfig(phases=[])
        env = Environment(resolved, config)
        env.run()

        inv = env.log.to_dataframes()["simulation_inventory_log"]
        totals = inv.groupby("period_index")["quantity"].sum()
        assert (totals == 69.0).all()  # 50 + 12 + 7


class TestEnvironmentWithNoopTask:
    """DispatchPhase(NoopTask) produces no dispatches — engine still works."""

    def test_noop_dispatch_no_side_effects(self) -> None:
        resolved = _build_resolved_with_inventory()
        config = EnvironmentConfig(
            phases=[DispatchPhase(task=NoopTask())],
        )
        env = Environment(resolved, config)
        log = env.run()

        dfs = log.to_dataframes()
        assert len(dfs["simulation_flow_log"]) == 0
        assert len(dfs["simulation_rejected_dispatches_log"]) == 0
        # Inventory unchanged
        inv = dfs["simulation_inventory_log"]
        totals = inv.groupby("period_index")["quantity"].sum()
        assert (totals == 69.0).all()


class TestFullPipelineIntegration:
    """Full simulation with DemandPhase + ArrivalsPhase + DispatchPhase(NoopTask)."""

    def test_demand_reduces_inventory(self) -> None:
        from gbp.consumers.simulator.built_in_phases import (
            ArrivalsPhase,
            DemandPhase,
        )

        resolved = _build_resolved_with_inventory()
        config = EnvironmentConfig(
            phases=[
                DemandPhase(),
                ArrivalsPhase(),
                DispatchPhase(task=NoopTask()),
            ],
        )
        env = Environment(resolved, config)
        log = env.run()

        dfs = log.to_dataframes()
        inv = dfs["simulation_inventory_log"]

        # After 3 periods of demand, total inventory should decrease
        p0_total = inv.loc[inv["period_index"] == 0, "quantity"].sum()
        p2_total = inv.loc[inv["period_index"] == 2, "quantity"].sum()
        assert p2_total < p0_total

        # Flow log should have demand events
        assert len(dfs["simulation_flow_log"]) > 0
