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

from gbp.consumers.simulator.state import PeriodRow, SimulationState
from gbp.consumers.simulator.validation import validate_run
from gbp.model import flows as J
from tests import scenarios
from tests.invariants import check_journal_well_formed


def _sorted(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(["period_id", "facility_id", "commodity_category"]).reset_index(drop=True)


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
    assert len(redirected_ids) > 0  # a redirect actually fired
    assert journal["move_id"].max() == 1  # a second arc exists

    # Every redirected flow shows exactly the four-event shape, in order.
    for flow_id in redirected_ids:
        events = (
            journal[journal["flow_id"] == flow_id].sort_values("event_id")["event_type"].tolist()
        )
        assert events == ["departed", "redirected", "departed", "arrived"]

    # The bounce docks nowhere; the second arc lands away from the full s3.
    bounces = journal[journal["event_type"] == "redirected"]
    assert bounces["realized_target_id"].isna().all()
    second_arc_arrivals = journal[(journal["event_type"] == "arrived") & (journal["move_id"] == 1)]
    assert (second_arc_arrivals["realized_target_id"] != "s3").all()


def test_stockout_logs_the_lost_demand(run_scenario):
    _resolved, journal, _state = run_scenario("stockout")
    stockout = journal[(journal["event_type"] == "lost") & (journal["reason"] == "stockout")]
    assert int(stockout["quantity"].sum()) == 3  # 5 wanted, 2 in stock, 3 lost
    departures = J.flows_to_departures(journal)
    s1_p0 = departures[(departures["facility_id"] == "s1") & (departures["period_id"] == 0)]
    assert int(s1_p0["quantity"].sum()) == 2  # only what the inventory covered


def test_network_full_loses_bikes_to_dock_full(run_scenario):
    _resolved, journal, _state = run_scenario("network_full")
    lost = journal[(journal["event_type"] == "lost") & (journal["reason"] == "dock_full")]
    assert int(lost["quantity"].sum()) == 3  # nowhere to dock -> all lost
    assert (journal["event_type"] != "arrived").all()  # nothing ever docked


def test_single_trip_is_one_departed_then_one_arrived(run_scenario):
    _resolved, journal, _state = run_scenario("single_trip")
    assert journal["event_type"].tolist() == ["departed", "arrived"]
    assert journal["move_id"].tolist() == [0, 0]
    assert journal["event_id"].tolist() == [0, 1]


# ---------------------------------------------------------------------------
# Moment-level inventory (step_id): the fine view agrees with the coarse one
# ---------------------------------------------------------------------------
def _period_end_inventory_from_moments(journal, initial):
    """Inventory at each period's last step -- should match get_inventory_df."""
    moments = J.inventory_at_moments(journal, initial)
    last = moments.groupby("period_id")["step_id"].transform("max") == moments["step_id"]
    end = moments[last][["period_id", "facility_id", "commodity_category", "inventory_after"]]
    return end.rename(columns={"inventory_after": "quantity_eop"})


@pytest.mark.parametrize("name", list(scenarios.ALL_SCENARIOS))
def test_stamped_step_id_matches_tuple_order(name, run_scenario):
    # The bridge (oracle) test. The simulator now stamps step_id when a phase opens
    # a step, instead of deriving it from the (period_id, phase_rank, phase_round)
    # tuple. For the phases that exist today -- all pure user trips -- the stamped
    # number must still equal the old tuple-derived one: same numbers, new
    # mechanism. We rebuild the tuple-derived number independently (recomputing
    # phase_rank with the timing rule, the oracle) and check the stamped step_id
    # matches it. A future rebalancer that emits two ordered batches under one
    # tuple is allowed to break this equality -- that is the whole point of
    # stamping -- and would simply not be a user-trip scenario.
    _resolved, journal, _state = run_scenario(name)
    if journal.empty:
        return
    keys = pd.DataFrame(
        {
            "period_id": journal["period_id"],
            "phase_rank": J.phase_rank_by_timing(journal),
            "phase_round": journal["phase_round"].fillna(0),
        }
    )
    # ngroup with sort=True numbers the tuples in sorted (= step) order.
    recomputed = keys.groupby(["period_id", "phase_rank", "phase_round"], sort=True).ngroup()
    assert recomputed.tolist() == journal["step_id"].tolist()


def test_open_step_gives_distinct_numbers_to_separate_opens():
    # The guarantee stamping buys over the old tuple derivation: two batches opened
    # separately always get different step_ids, even if their (period_id,
    # phase_rank, phase_round) label is identical. No phase does this today (it is
    # what a future rebalancer needs), so we lock it at the source -- open_step.
    state = SimulationState(
        state_period_id_obj=PeriodRow(0, None, None),
        state_inventory_df=pd.DataFrame(),
        state_flows_df=pd.DataFrame(),
        state_resources_df=pd.DataFrame(),
    )
    first, state = state.open_step()
    second, state = state.open_step()
    third, state = state.open_step()
    assert [first, second, third] == [0, 1, 2]
    assert state.next_step_id == 3


@pytest.mark.parametrize("name", list(scenarios.ALL_SCENARIOS))
def test_moments_last_step_matches_per_period_inventory(name, run_scenario):
    # The per-period inventory (get_inventory_df) is the value at each period's
    # last step, so the fine inventory_at_moments must coarsen back to it exactly.
    resolved, journal, _state = run_scenario(name)
    cols = ["period_id", "facility_id", "commodity_category", "quantity_eop"]
    coarse = _sorted(J.get_inventory_df(journal, resolved.initial_inventory_df))[cols]
    fine = _sorted(_period_end_inventory_from_moments(journal, resolved.initial_inventory_df))[cols]
    pd.testing.assert_frame_equal(coarse, fine, check_dtype=False)


def test_redirect_rounds_are_distinct_steps_that_see_earlier_rounds():
    # Five bikes overflow the full s3; the nearest free neighbour s2 holds two,
    # then s4 holds two, then s5 -- so the redirect spans three rounds. Each round
    # is its own step, and the step that docks into s4 must already see s2 full.
    # (Filler trips in period 6 only exist to create facilities s2/s4/s5.)
    trips = [("s1", "s3", 0, 1)] * 5 + [("s1", "s2", 6, 7), ("s1", "s4", 6, 7), ("s1", "s5", 6, 7)]
    resolved = scenarios.build_resolved(
        trips,
        capacities={"s3": 0, "s2": 2, "s4": 2, "s5": 10},
        initial_inventory={"s1": 8},
    )
    journal, _state = scenarios.run(resolved)
    init = resolved.initial_inventory_df

    wide = J.flows_with_inventory(journal, init)
    dockings = wide[
        (wide["move_id"] == 1) & (wide["event_type"] == "arrived") & (wide["period_id"] == 1)
    ]
    assert dockings["step_id"].nunique() == 3  # one distinct step per round

    s2_step = int(dockings.loc[dockings["realized_target_id"] == "s2", "step_id"].iloc[0])
    s4_step = int(dockings.loc[dockings["realized_target_id"] == "s4", "step_id"].iloc[0])
    assert s4_step > s2_step  # s4 is filled in a later round

    moments = J.inventory_at_moments(journal, init)
    s2_at_s4_step = moments[(moments["step_id"] == s4_step) & (moments["facility_id"] == "s2")]
    assert int(s2_at_s4_step["inventory_before"].iloc[0]) == 2  # s2 already full by then


def _multi_round_redirect():
    trips = [("s1", "s3", 0, 1)] * 5 + [("s1", "s2", 6, 7), ("s1", "s4", 6, 7), ("s1", "s5", 6, 7)]
    resolved = scenarios.build_resolved(
        trips, capacities={"s3": 0, "s2": 2, "s4": 2, "s5": 10}, initial_inventory={"s1": 8}
    )
    journal, _state = scenarios.run(resolved)
    return resolved, journal


def test_redirect_neighbor_table_shows_the_nearer_neighbour_was_full():
    # Explain the redirect that docked at s4: the nearer s2 must show as full at
    # the moment, which is why the bike skipped it, and s4 must be the last row
    # (the table stops at the station the bike actually reached).
    resolved, journal = _multi_round_redirect()
    init, geo, caps = (
        resolved.initial_inventory_df,
        resolved.facilities_geo_df,
        resolved.facilities_capacities_df,
    )
    wide = J.flows_with_inventory(journal, init)
    docks = wide[
        (wide["move_id"] == 1) & (wide["event_type"] == "arrived") & (wide["period_id"] == 1)
    ]
    flow_id = docks.loc[docks["realized_target_id"] == "s4", "flow_id"].iloc[0]

    table = J.redirect_neighbor_table(journal, init, geo, flow_id, capacities=caps)
    assert table["neighbor_rank"].tolist() == list(range(len(table)))  # nearest-first
    assert table["distance_sq"].is_monotonic_increasing
    assert table["facility_id"].iloc[-1] == "s4"  # cut at C inclusive
    assert (table["realized_target_id"] == "s4").all()

    s2 = table[table["facility_id"] == "s2"].iloc[0]
    s4 = table[table["facility_id"] == "s4"].iloc[0]
    assert int(s2["free_before"]) == 0  # the nearer neighbour was full
    assert int(s4["free_before"]) >= 1  # the bike's landing had room


def test_redirect_neighbor_table_rejects_a_non_redirect_flow():
    resolved, journal = _multi_round_redirect()
    plain = journal[(journal["event_type"] == "arrived") & (journal["move_id"] == 0)]
    flow_id = plain["flow_id"].iloc[0]
    with pytest.raises(ValueError, match="no redirect"):
        J.redirect_neighbor_table(
            journal, resolved.initial_inventory_df, resolved.facilities_geo_df, flow_id
        )
