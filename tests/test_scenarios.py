"""End-to-end scenario tests: run the real engine, assert the invariants.

Two layers:

1. A universal sweep -- every scenario, run through the real three-phase
   :class:`Environment`, must produce a structurally well-formed journal
   (:mod:`tests.invariants`) and satisfy the run invariants I1-I5
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

from gbp.consumers.simulator.config import EnvironmentConfig
from gbp.consumers.simulator.engine import Environment
from gbp.consumers.simulator.phases import DockArrivals, FormDeparturesPhase
from gbp.consumers.simulator.state import PeriodRow, SimulationState, SimulatorConfigError
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


def test_delayed_redirect_docks_in_a_later_period(run_scenario):
    # Scenario 5 of docs/scenarios.md: the bounce happens in the
    # flow's opening period, the new leg takes two periods (the OD travel time of
    # the s3 -> s2 pair), and the bike docks at s2 with the normal dock batch of
    # the arrival period.
    _resolved, journal, _state = run_scenario("overflow_delayed")
    flow_id = journal.loc[journal["event_type"] == "redirected", "flow_id"].iloc[0]
    flow = journal[journal["flow_id"] == flow_id]
    assert flow["event_type"].tolist() == ["departed", "redirected", "departed", "arrived"]
    assert flow["period_id"].tolist() == [0, 0, 0, 2]
    assert flow["phase_rank"].tolist() == [1, 2, 2, 0]
    assert flow["phase_round"].tolist() == [0, 1, 1, 0]
    # The bounce and the leg's departure share one step; the delayed docking is
    # a later one, shared with the arrival period's dock batch (the s3 -> s2 trip).
    steps = flow["step_id"].tolist()
    assert steps[1] == steps[2] and steps[0] < steps[1] < steps[3]
    arrival = flow.iloc[3]
    assert arrival["realized_target_id"] == "s2"
    batch_mates = journal[
        (journal["event_type"] == "arrived")
        & (journal["move_id"] == 0)
        & (journal["period_id"] == 2)
    ]
    assert int(arrival["step_id"]) == int(batch_mates["step_id"].iloc[0])


def test_redirect_chain_bounces_again_on_arrival(run_scenario):
    # Scenario 7 timing plus a second redirect: the first leg is due at t+1, the
    # bounce leg rides two more periods, and the station it heads to fills up
    # meanwhile -- so the bike bounces a second time and docks at s1.
    _resolved, journal, _state = run_scenario("redirect_chain")
    bounces = journal[journal["event_type"] == "redirected"]
    flow_id = bounces["flow_id"].iloc[0]
    assert len(bounces) == 2 and (bounces["flow_id"] == flow_id).all()
    flow = journal[journal["flow_id"] == flow_id]
    assert flow["event_type"].tolist() == [
        "departed",
        "redirected",
        "departed",
        "redirected",
        "departed",
        "arrived",
    ]
    assert flow["move_id"].tolist() == [0, 0, 1, 1, 2, 2]
    assert flow["event_id"].tolist() == list(range(6))
    assert flow["period_id"].tolist() == [0, 1, 1, 3, 3, 3]
    assert bounces["planned_target_id"].tolist() == ["s4", "s3"]
    assert flow.iloc[-1]["realized_target_id"] == "s1"


def test_stockout_logs_the_lost_demand(run_scenario):
    _resolved, journal, _state = run_scenario("stockout")
    stockout = journal[(journal["event_type"] == "lost") & (journal["reason"] == "stockout")]
    assert int(stockout["quantity"].sum()) == 3  # 5 wanted, 2 in inventory, 3 lost
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
    """Inventory at each period's last step -- should match get_inventory_df.

    A period with no step (nothing moved; e.g. every bike is riding) keeps the
    previous period's value, so the values are carried forward over the full
    period range before comparing.
    """
    moments = J.inventory_at_moments(journal, initial)
    last = moments.groupby("period_id")["step_id"].transform("max") == moments["step_id"]
    end = moments[last][["period_id", "facility_id", "commodity_category", "inventory_after"]]
    wide = end.pivot_table(
        index=["facility_id", "commodity_category"], columns="period_id", values="inventory_after"
    ).reindex(columns=range(int(journal["period_id"].max()) + 1))
    return wide.ffill(axis=1).stack().rename("quantity_eop").reset_index()


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


def _state_with(inventory: dict[str, int]) -> SimulationState:
    inv = pd.DataFrame(
        {
            "facility_id": pd.Series(list(inventory), dtype="string"),
            "commodity_category": pd.Series([scenarios.CLASSIC] * len(inventory), dtype="string"),
            "quantity": pd.Series(list(inventory.values()), dtype="int64"),
        }
    )
    return SimulationState(
        state_period_id_obj=PeriodRow(0, None, None),
        state_inventory_df=inv,
        state_flows_df=J.empty_flows_journal(),
        state_resources_df=pd.DataFrame(),
    )


def _trips(rows: list[tuple[str, str, str, int, int]]) -> pd.DataFrame:
    # One row per trip: (flow_id, source, target, start_period, end_period).
    return pd.DataFrame(
        {
            "flow_id": [r[0] for r in rows],
            "source_id": [r[1] for r in rows],
            "planned_target_id": [r[2] for r in rows],
            "commodity_category": scenarios.CLASSIC,
            "start_period": [r[3] for r in rows],
            "planned_end_period": [r[4] for r in rows],
        }
    )


def _quantity(state: SimulationState, facility: str) -> int:
    inv = state.state_inventory_df
    return int(inv.loc[inv["facility_id"] == facility, "quantity"].sum())


def test_apply_step_events_opens_one_step_per_round_and_stamps_all_three_columns():
    # The single write path for a phase: one step per distinct phase_round (rows
    # without a round are round 0), phase_rank on every row, and the rows appended
    # to the journal.
    departed = J.departed_events(
        _trips([("f0", "s1", "s2", 0, 1), ("f1", "s1", "s2", 0, 1), ("f2", "s1", "s2", 0, 1)])
    )
    events = departed.assign(phase_round=[pd.NA, 1, 1])
    new_state = _state_with({"s1": 3}).apply_step_events(events, phase_rank=2)
    written = new_state.state_flows_df
    assert written["phase_rank"].tolist() == [2, 2, 2]
    assert written["phase_round"].tolist() == [0, 1, 1]
    assert written["step_id"].tolist() == [0, 1, 1]
    assert new_state.next_step_id == 2


def test_apply_step_events_with_no_events_opens_no_step():
    # A step opened for nothing would leave a gap in the numbering.
    state = _state_with({})
    new_state = state.apply_step_events(state.state_flows_df.iloc[:0], phase_rank=1)
    assert new_state.next_step_id == 0
    assert new_state.state_flows_df.empty


def test_apply_step_events_moves_departures_out_of_inventory_and_into_in_transit():
    # Events in, state out: writing a departed batch is enough -- the same call
    # takes the bikes off the docks and puts the rows into the in-transit set.
    departed = J.departed_events(_trips([("f0", "s1", "s2", 0, 1), ("f1", "s1", "s3", 0, 2)]))
    new_state = _state_with({"s1": 3}).apply_step_events(departed, phase_rank=1)
    assert _quantity(new_state, "s1") == 1
    assert new_state.in_transit["flow_id"].tolist() == ["f0", "f1"]


def test_apply_step_events_docks_arrivals_and_closes_their_in_transit_rows():
    departed = J.departed_events(_trips([("f0", "s1", "s2", 0, 1)]))
    state = _state_with({"s1": 1}).apply_step_events(departed, phase_rank=1)
    arrived = J.arrived_events(state.in_transit, 1)
    new_state = state.apply_step_events(arrived, phase_rank=0)
    assert _quantity(new_state, "s2") == 1
    assert new_state.in_transit.empty


def test_apply_step_events_swaps_a_bounced_arc_and_moves_no_inventory():
    # A redirect bounce closes arc 0 and opens arc 1 in one batch: the in-transit
    # set swaps the old arc's row for the new leg, and no bike docks or undocks.
    departed = J.departed_events(_trips([("f0", "s1", "s2", 0, 1)]))
    state = _state_with({"s1": 1}).apply_step_events(departed, phase_rank=1)
    due = state.in_transit
    bounce = J.redirected_events(due, 1)
    leg = J.redirect_leg_events(due.assign(realized_target_id="s3", leg_end_period=2), 1)
    new_state = state.apply_step_events(pd.concat([bounce, leg], ignore_index=True), phase_rank=0)
    assert int(new_state.state_inventory_df["quantity"].sum()) == 0
    assert new_state.in_transit["move_id"].tolist() == [1]
    assert new_state.in_transit["planned_target_id"].tolist() == ["s3"]


def test_apply_step_events_zero_duration_leg_never_enters_in_transit():
    # An arc opened and closed inside the same batch (a zero-travel redirect leg)
    # docks immediately: the bike lands at the leg's target and nothing rides on.
    departed = J.departed_events(_trips([("f0", "s1", "s2", 0, 1)]))
    state = _state_with({"s1": 1}).apply_step_events(departed, phase_rank=1)
    due = state.in_transit
    bounce = J.redirected_events(due, 1)
    leg = J.redirect_leg_events(due.assign(realized_target_id="s3", leg_end_period=1), 1)
    arrived = J.arrived_events(leg, 1)
    batch = pd.concat([bounce, leg, arrived], ignore_index=True)
    new_state = state.apply_step_events(batch, phase_rank=0)
    assert _quantity(new_state, "s3") == 1
    assert new_state.in_transit.empty


def test_engine_rejects_phases_out_of_rank_order():
    # The list position hands out step_id and the declared phase_rank sorts the
    # steps, so the two orders must agree. An out-of-order list would write a
    # journal whose step_id and phase_rank disagree; the engine refuses it.
    resolved = scenarios.build_resolved([("s1", "s2", 0, 1)])
    config = EnvironmentConfig(
        phases=[FormDeparturesPhase(), DockArrivals("previous")],
        scenario_id="test",
        number_of_periods=1,
    )
    with pytest.raises(SimulatorConfigError, match="ordered by phase_rank"):
        Environment(resolved, config)


def test_apply_step_events_stockout_lost_moves_nothing():
    # A stockout lost is pure accounting: no flow_id, no inventory move, no
    # in-transit entry -- only the journal grows.
    losses = pd.DataFrame(
        {"source_id": ["s1"], "commodity_category": [scenarios.CLASSIC], "quantity": [2]}
    )
    state = _state_with({"s1": 1})
    new_state = state.apply_step_events(J.lost_events(losses, 0, "stockout"), phase_rank=1)
    assert _quantity(new_state, "s1") == 1
    assert new_state.in_transit.empty
    assert len(new_state.state_flows_df) == 1


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


# ---------------------------------------------------------------------------
# flows_with_costs: riding time and accrued money per event
# ---------------------------------------------------------------------------
_RATES = pd.DataFrame({"commodity_category": [scenarios.CLASSIC], "rate": [3.0]})
_ONE_HOUR = pd.Timedelta(hours=1)


@pytest.mark.parametrize("name", list(scenarios.ALL_SCENARIOS))
def test_costs_follow_the_period_columns(name, run_scenario):
    # elapsed_periods is period_id - start_period on every row: 0 on the opening
    # departed, and realized_end_period - start_period on the events that close
    # an arc (arrived, redirected). cost is rate * elapsed hours (1h periods).
    _resolved, journal, _state = run_scenario(name)
    priced = J.flows_with_costs(journal, _RATES, _ONE_HOUR)

    opening = priced[(priced["event_type"] == "departed") & (priced["move_id"] == 0)]
    assert (opening["elapsed_periods"] == 0).all()

    closing = priced[priced["event_type"].isin(["arrived", "redirected"])]
    assert (
        closing["elapsed_periods"] == closing["realized_end_period"] - closing["start_period"]
    ).all()

    # NA only where the flow never rode: a stockout loss has no start_period.
    na_rows = priced[priced["elapsed_periods"].isna()]
    assert (na_rows["reason"] == "stockout").all()
    rode = priced[priced["elapsed_periods"].notna()]
    assert (rode["cost"] == 3.0 * rode["elapsed_periods"]).all()

    # Riding time never decreases along one flow's events.
    ordered = rode.sort_values(["flow_id", "event_id"])
    assert (ordered.groupby("flow_id")["elapsed_periods"].diff().dropna() >= 0).all()


def test_costs_accumulate_over_redirect_legs(run_scenario):
    # redirect_chain: the bike leaves s1 at period 0, reaches the full s4 at
    # period 1 (bounce), rides two periods toward s3, bounces again at period 3
    # and docks at s1 in that same period. elapsed_periods must carry the sum of
    # the legs at each event, not restart per leg.
    _resolved, journal, _state = run_scenario("redirect_chain")
    priced = J.flows_with_costs(journal, _RATES, _ONE_HOUR)

    opening = priced[(priced["event_type"] == "departed") & (priced["move_id"] == 0)]
    flow_id = opening.loc[opening["source_id"] == "s1", "flow_id"].iloc[0]
    flow = priced[priced["flow_id"] == flow_id].sort_values("event_id")

    assert flow["event_type"].tolist() == [
        "departed",
        "redirected",
        "departed",
        "redirected",
        "departed",
        "arrived",
    ]
    assert flow["elapsed_periods"].tolist() == [0, 1, 1, 3, 3, 3]
    assert flow["cost"].tolist() == [0.0, 3.0, 3.0, 9.0, 9.0, 9.0]


def test_costs_work_on_the_historical_journal(run_scenario):
    # The same read-model prices the historical journal, where no simulator ran.
    resolved, _journal, _state = run_scenario("canonical")
    priced = J.flows_with_costs(resolved.historical_flows_df, _RATES, _ONE_HOUR)
    arrived = priced[priced["event_type"] == "arrived"]
    assert (
        arrived["elapsed_periods"] == arrived["planned_end_period"] - arrived["start_period"]
    ).all()
    assert (arrived["cost"] == 3.0 * arrived["elapsed_periods"]).all()


def test_cost_converts_periods_to_hours(run_scenario):
    # The rate is dollars per hour; with 30-minute periods every cost halves.
    _resolved, journal, _state = run_scenario("single_trip")
    full = J.flows_with_costs(journal, _RATES, _ONE_HOUR)
    half = J.flows_with_costs(journal, _RATES, pd.Timedelta(minutes=30))
    assert (half["cost"] == full["cost"] / 2).all()
