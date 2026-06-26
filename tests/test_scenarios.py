"""End-to-end scenario tests: run the real engine, assert the invariants.

Two layers:

1. A universal sweep -- every scenario, run through the real four-phase
   :class:`Environment`, must produce a structurally well-formed journal
   (:mod:`tests.invariants`) and satisfy the run invariants I1-I4
   (:func:`gbp.consumers.simulator.validation.validate_run`). These hold by
   design on *any* run, so the same two assertions cover every scenario.

2. Per-scenario properties -- the specific thing each scenario is built to
   exercise (a redirect fires, a stockout is logged, the canonical run
   reproduces history), checked once where it is meaningful. The universal
   sweep proves "nothing is malformed"; these prove "the right thing happened".

Every test pulls its run from the ``run_scenario`` fixture (in ``conftest.py``),
which builds and simulates each scenario once and caches it, so the shared setup
is not repeated while the assertions stay separate -- one test, one reason to
fail.
"""

import pandas as pd
import pytest

from gbp.consumers.simulator.validation import validate_run
from gbp.model import flows as J
from tests import scenarios
from tests.invariants import check_journal_well_formed


def _sorted(df: pd.DataFrame) -> pd.DataFrame:
    return (df.sort_values(["period_id", "facility_id", "commodity_category"])
            .reset_index(drop=True))


# ---------------------------------------------------------------------------
# Universal: every scenario is well-formed and conserves bikes
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("name", list(scenarios.ALL_SCENARIOS))
def test_journal_is_well_formed(name, run_scenario):
    _resolved, journal, _state = run_scenario(name)
    assert check_journal_well_formed(journal) == []


@pytest.mark.parametrize("name", list(scenarios.ALL_SCENARIOS))
def test_run_invariants_hold(name, run_scenario):
    resolved, _journal, state = run_scenario(name)
    assert validate_run(state, resolved) == []


# ---------------------------------------------------------------------------
# Per-scenario properties
# ---------------------------------------------------------------------------
def test_canonical_has_no_constraints_and_reproduces_demand(run_scenario):
    resolved, journal, _state = run_scenario("canonical")
    # No constraint binds: every flow is a single normal arc.
    assert journal["move_id"].max() == 0
    assert set(journal["event_type"]) == {"departed", "arrived"}
    # The base-replay invariant: the simulated departures marginal equals history.
    pd.testing.assert_frame_equal(
        _sorted(J.flows_to_departures(journal)),
        _sorted(resolved.historical_demand_df),
    )


def test_overflow_produces_the_four_event_redirect(run_scenario):
    _resolved, journal, _state = run_scenario("overflow")
    redirected_ids = journal.loc[journal["event_type"] == "redirected", "flow_id"].unique()
    assert len(redirected_ids) > 0                 # a redirect actually fired
    assert journal["move_id"].max() == 1           # a second arc exists

    # Every redirected flow shows exactly the four-event shape, in order.
    for flow_id in redirected_ids:
        events = (journal[journal["flow_id"] == flow_id]
                  .sort_values("event_id")["event_type"].tolist())
        assert events == ["departed", "redirected", "departed", "arrived"]

    # The bounce docks nowhere; the second arc lands away from the full s3.
    bounces = journal[journal["event_type"] == "redirected"]
    assert bounces["realized_target_id"].isna().all()
    second_arc_arrivals = journal[(journal["event_type"] == "arrived") & (journal["move_id"] == 1)]
    assert (second_arc_arrivals["realized_target_id"] != "s3").all()


def test_stockout_logs_the_lost_demand(run_scenario):
    _resolved, journal, _state = run_scenario("stockout")
    stockout = journal[(journal["event_type"] == "lost") & (journal["reason"] == "stockout")]
    assert int(stockout["quantity"].sum()) == 3    # 5 wanted, 2 in stock, 3 lost
    departures = J.flows_to_departures(journal)
    s1_p0 = departures[(departures["facility_id"] == "s1") & (departures["period_id"] == 0)]
    assert int(s1_p0["quantity"].sum()) == 2       # only what the inventory covered


def test_network_full_loses_bikes_to_dock_full(run_scenario):
    _resolved, journal, _state = run_scenario("network_full")
    lost = journal[(journal["event_type"] == "lost") & (journal["reason"] == "dock_full")]
    assert int(lost["quantity"].sum()) == 3        # nowhere to dock -> all lost
    assert (journal["event_type"] != "arrived").all()  # nothing ever docked


def test_single_trip_is_one_departed_then_one_arrived(run_scenario):
    _resolved, journal, _state = run_scenario("single_trip")
    assert journal["event_type"].tolist() == ["departed", "arrived"]
    assert journal["move_id"].tolist() == [0, 0]
    assert journal["event_id"].tolist() == [0, 1]
