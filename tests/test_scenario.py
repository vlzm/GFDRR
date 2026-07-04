"""Direct tests for the size-then-run entry point (``run_sized_scenario``).

Until now the size -> run -> validate order lived only in ``app/runner.py`` and
the notebook, with no direct test. These tests pin the contract of the one
function that owns that order: equal scale factors reproduce the demand with no
loss, the scenario data it was given is not mutated, and a run scale above the
sizing scale makes the limits take effect while the run invariants still hold.
"""

import pandas as pd

from gbp.consumers.simulator import run_sized_scenario
from gbp.model import flows_to_departures
from tests import scenarios

KEYS = ["period_id", "facility_id", "commodity_category"]


def _run(resolved, **kwargs):
    return run_sized_scenario(
        resolved,
        scenario_id="scenario_test",
        number_of_periods=len(resolved.periods_df),
        **kwargs,
    )


def test_equal_scales_reproduce_the_demand_with_no_loss():
    # sizing_scale == run_scale is the base replay: nothing is lost or
    # redirected, the invariants hold, and the simulated departures equal the
    # historical ones.
    resolved = scenarios.canonical()
    result = _run(resolved)
    assert result.violations == []
    assert not result.simulated_flows_df["event_type"].isin(["lost", "redirected"]).any()
    simulated = flows_to_departures(result.simulated_flows_df)
    historical = resolved.historical_demand_df
    pd.testing.assert_frame_equal(
        simulated.sort_values(KEYS).reset_index(drop=True),
        historical.sort_values(KEYS).reset_index(drop=True),
        check_dtype=False,
    )


def test_run_does_not_mutate_the_scenario_data():
    # The run works on a shallow copy: the sized tables come back on the
    # result, and the caller's ``resolved`` keeps its own state. The Run page
    # reuses one cached ``graph_data`` across runs, so this must hold.
    resolved = scenarios.stockout()
    inventory_before = resolved.initial_inventory_df.copy()
    capacities_before = resolved.facilities_capacities_df.copy()
    result = _run(resolved)
    pd.testing.assert_frame_equal(resolved.initial_inventory_df, inventory_before)
    pd.testing.assert_frame_equal(resolved.facilities_capacities_df, capacities_before)
    # The sized tables differ from the input ones -- they came from the sizing
    # run, not from the caller's state.
    assert int(result.initial_inventory_df["quantity"].sum()) != int(
        inventory_before["quantity"].sum()
    )


def test_run_scale_above_sizing_scale_hits_the_limits():
    # The state is sized for 1x demand but the run faces 2x: some departures
    # find no bike (lost events appear), yet the run invariants still hold, so
    # validate=True (the default) does not raise.
    resolved = scenarios.canonical()
    result = _run(resolved, demand_scale_factor=2.0, sizing_scale_factor=1.0)
    assert (result.simulated_flows_df["event_type"] == "lost").any()
    assert result.violations == []
