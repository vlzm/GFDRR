"""Direct tests for the sizing run (``size_state_for_demand``).

The sizing run measures the smallest initial inventory and dock capacities a
scenario's demand needs. Until now only the notebook drove it; these tests pin
its contract directly: a run started from the measured state repeats the demand
with no stockout and no dock-full, the measurement does not mutate the scenario
data it was given, and the measured state grows with the demand scale.
"""

import copy

import pandas as pd
import pytest

from gbp.consumers.simulator import run_sized_scenario, size_state_for_demand
from gbp.consumers.simulator.config import EnvironmentConfig
from gbp.consumers.simulator.validation import validate_run
from tests import scenarios


def _config(resolved, demand_scale_factor: float = 1.0) -> EnvironmentConfig:
    return EnvironmentConfig(
        phases=scenarios.canonical_phases(),
        scenario_id="sizing_test",
        validate=False,
        demand_scale_factor=demand_scale_factor,
        number_of_periods=len(resolved.periods_df),
    )


@pytest.mark.parametrize("name", ["stockout", "overflow", "canonical"])
def test_sized_state_runs_clean(name):
    # The sizing contract: a run started from the measured state loses nothing --
    # no stockout, no redirect, no dock-full -- and satisfies the run invariants.
    resolved = scenarios.ALL_SCENARIOS[name]()
    inventory, capacities = size_state_for_demand(resolved, _config(resolved))
    resolved.initial_inventory_df = inventory
    resolved.facilities_capacities_df = capacities
    journal, state = scenarios.run(resolved)
    assert not journal["event_type"].isin(["lost", "redirected"]).any()
    assert validate_run(state, resolved) == []


def test_sizing_does_not_mutate_the_scenario_data():
    # ``size_state_for_demand`` promises to leave ``resolved`` untouched: the
    # caller assigns the returned tables explicitly.
    resolved = scenarios.stockout()
    inventory_before = resolved.initial_inventory_df.copy()
    capacities_before = resolved.facilities_capacities_df.copy()
    size_state_for_demand(resolved, _config(resolved))
    pd.testing.assert_frame_equal(resolved.initial_inventory_df, inventory_before)
    pd.testing.assert_frame_equal(resolved.facilities_capacities_df, capacities_before)


def test_sized_inventory_grows_with_the_demand_scale():
    # Doubling the demand must not shrink the measured start inventory.
    resolved = scenarios.stockout()
    inventory_1x, _ = size_state_for_demand(resolved, _config(resolved))
    inventory_2x, _ = size_state_for_demand(resolved, _config(resolved, demand_scale_factor=2.0))
    assert int(inventory_2x["quantity"].sum()) > int(inventory_1x["quantity"].sum())


def test_forecast_sized_run_loses_the_underpredicted_demand():
    # A forecast-sized run (Notations.md §11): the state is sized on one demand
    # table (the forecast), the run faces another (the actual). Here the
    # "forecast" sees 2 of the 5 stockout-scenario departures, so the sized
    # inventory holds 2 bikes and the other 3 departures are lost — and the
    # run invariants still hold (losses are legal outcomes, not violations).
    resolved = scenarios.stockout()
    underprediction = copy.copy(resolved)
    underprediction.historical_demand_df = resolved.historical_demand_df.assign(quantity=2)

    result = run_sized_scenario(
        resolved,
        scenario_id="forecast_sized_test",
        number_of_periods=len(resolved.periods_df),
        validate=False,
        sizing_data=underprediction,
    )

    lost = result.simulated_flows_df[result.simulated_flows_df["event_type"] == "lost"]
    assert int(lost["quantity"].sum()) == 3
    assert result.violations == []
    assert int(result.initial_inventory_df["quantity"].sum()) == 2
