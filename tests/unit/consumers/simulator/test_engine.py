"""Tests for Environment engine and EnvironmentConfig."""
# ruff: noqa: D102

from __future__ import annotations

import pytest

from gbp.consumers.simulator.config import EnvironmentConfig
from gbp.consumers.simulator.engine import Environment
from gbp.core.model import ResolvedModelData


class TestEnvironmentEmptyPhases:
    """Environment with no phases — pure iteration over periods."""

    def test_run_returns_log(self, resolved_model: ResolvedModelData) -> None:
        config = EnvironmentConfig(phases=[])
        env = Environment(resolved_model, config)
        log = env.run()

        dfs = log.to_dataframes()
        inv_log = dfs["simulation_inventory_log"]
        # 3 periods x 3 facilities = 9 rows
        assert len(inv_log) == 9

    def test_is_done_after_run(self, resolved_model: ResolvedModelData) -> None:
        config = EnvironmentConfig(phases=[])
        env = Environment(resolved_model, config)

        assert not env.is_done
        env.run()
        assert env.is_done

    def test_step_advances_period(self, resolved_model: ResolvedModelData) -> None:
        config = EnvironmentConfig(phases=[])
        env = Environment(resolved_model, config)

        assert env.state.period_index == 0
        env.step()
        assert env.state.period_index == 1
        env.step()
        assert env.state.period_index == 2
        env.step()
        assert env.is_done

    def test_step_raises_when_done(self, resolved_model: ResolvedModelData) -> None:
        config = EnvironmentConfig(phases=[])
        env = Environment(resolved_model, config)
        env.run()

        with pytest.raises(StopIteration):
            env.step()

    def test_resource_log_shape(self, resolved_model: ResolvedModelData) -> None:
        config = EnvironmentConfig(phases=[])
        env = Environment(resolved_model, config)
        env.run()

        dfs = env.log.to_dataframes()
        res_log = dfs["simulation_resource_log"]
        # 3 periods x 3 trucks = 9 rows
        assert len(res_log) == 9

    def test_inventory_preserved_across_periods(
        self, resolved_model: ResolvedModelData
    ) -> None:
        """Without any phases, inventory stays constant across periods."""
        config = EnvironmentConfig(phases=[])
        env = Environment(resolved_model, config)
        env.run()

        dfs = env.log.to_dataframes()
        inv = dfs["simulation_inventory_log"]
        # Total inventory same each period
        totals = inv.groupby("period_index")["quantity"].sum()
        assert (totals == 50.0 + 12.0 + 7.0).all()


class TestStepPhase:
    """step_phase executes a single named phase."""

    def test_unknown_phase_raises(self, resolved_model: ResolvedModelData) -> None:
        config = EnvironmentConfig(phases=[])
        env = Environment(resolved_model, config)

        with pytest.raises(ValueError, match="No phase named"):
            env.step_phase("NONEXISTENT")
