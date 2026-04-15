"""Tests for ``Environment.__init__`` preconditions."""

from __future__ import annotations

import dataclasses

import pytest

from gbp.build.pipeline import build_model
from gbp.consumers.simulator.config import EnvironmentConfig
from gbp.consumers.simulator.engine import Environment
from gbp.consumers.simulator.exceptions import SimulatorConfigError
from tests.unit.build.fixtures import minimal_raw_model


def test_raises_when_all_flow_inputs_empty() -> None:
    """No demand, supply, or inventory_initial → the simulator cannot run."""
    raw = minimal_raw_model(with_demand=False, with_supply=False)
    resolved = build_model(raw)

    # Strip any flow inputs that might have been derived or carried in raw.
    resolved.demand = None
    resolved.supply = None
    resolved.inventory_initial = None

    with pytest.raises(SimulatorConfigError, match="demand, supply, or inventory_initial"):
        Environment(resolved, EnvironmentConfig(phases=[]))


def test_accepts_only_inventory_initial() -> None:
    """Inventory alone is enough to initialise the environment."""
    import pandas as pd

    raw = minimal_raw_model(with_demand=False, with_supply=False)
    raw = dataclasses.replace(
        raw,
        inventory_initial=pd.DataFrame(
            {
                "facility_id": ["d1"],
                "commodity_category": ["working_bike"],
                "quantity": [10.0],
            }
        ),
    )
    resolved = build_model(raw)
    resolved.demand = None
    resolved.supply = None

    env = Environment(resolved, EnvironmentConfig(phases=[]))
    assert env.state is not None


def test_accepts_only_demand() -> None:
    """Demand alone is enough — simulator can still run (stockouts are valid)."""
    raw = minimal_raw_model(with_demand=True, with_supply=False)
    resolved = build_model(raw)
    resolved.supply = None
    resolved.inventory_initial = None

    env = Environment(resolved, EnvironmentConfig(phases=[]))
    assert env.state is not None
