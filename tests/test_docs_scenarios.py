"""Every toy table in docs/scenarios.md, replayed against the real engine.

docs/scenarios.md shows one journal table per canonical scenario. Each table
sits under an anchor comment (``<!-- table:scenario-04 -->``). A test here
rebuilds the same scenario, runs the real engine, selects the same rows from
the fresh journal, and compares them with the table parsed out of the
document, cell by cell. If the code changes a journal shape, these tests go
red before the document can drift.

The scenario builders mirror the "Setup" line of each scenario in the
document; the shared machinery (trip history, rebalancing tables, scripted
truck routes) comes from :mod:`tests.scenarios`.
"""

import functools
from pathlib import Path

import pandas as pd
import pytest

from gbp.consumers.simulator import canonical_phases, rebalancing_phases
from gbp.consumers.simulator.rebalancing import RebalancingParams
from gbp.consumers.simulator.validation import validate_run
from gbp.model import flows as J
from tests import scenarios
from tests.invariants import check_journal_well_formed

DOC_PATH = Path(__file__).resolve().parent.parent / "docs" / "scenarios.md"

#: Columns of a user-trip table in the document.
TRIP_COLUMNS = [
    "event_id",
    "move_id",
    "event_type",
    "reason",
    "source_id",
    "planned_target_id",
    "realized_target_id",
    "period_id",
    "start_period",
    "phase_rank",
    "phase_round",
    "step_id",
]

#: Columns of a rebalance table in the document.
REBALANCE_COLUMNS = [
    "flow_id",
    "event_id",
    "event_type",
    "source_id",
    "planned_target_id",
    "realized_target_id",
    "period_id",
    "start_period",
    "phase_rank",
    "phase_round",
    "step_id",
]


# ---------------------------------------------------------------------------
# The scenarios, exactly as the document's "Setup" lines describe them
# ---------------------------------------------------------------------------
def _run(resolved, phases=None):
    journal, state = scenarios.run(resolved, phases=phases)
    return resolved, journal, state


def _run_rebalancing(trips, *, stops, capacities=None, initial_inventory):
    """Run a rebalancing story with a fixed truck route in place of the solver."""
    resolved = scenarios.with_rebalancing_data(
        scenarios.build_resolved(trips, capacities=capacities, initial_inventory=initial_inventory)
    )
    phases = canonical_phases() + rebalancing_phases(
        RebalancingParams(), lambda nodes, travel_minutes, trucks, params: stops
    )
    return _run(resolved, phases)


def _failing_solver(nodes, travel_minutes, trucks, params):
    raise AssertionError("the solver must not be called when nothing is short")


def _run_nothing_to_move():
    resolved = scenarios.with_rebalancing_data(
        scenarios.build_resolved([("s1", "s2", 6, 7)], initial_inventory={"s1": 5, "s2": 5})
    )
    phases = canonical_phases() + rebalancing_phases(RebalancingParams(), _failing_solver)
    return _run(resolved, phases)


#: Three morning trips leaving s2 at 06:00 -- the shared rebalancing story.
MORNING_TRIPS = [("s2", "s1", 6, 7)] * 3

DOC_SCENARIOS = {
    "scenario-01": lambda: _run(scenarios.build_resolved([("s1", "s2", 0, 1)])),
    "scenario-02": lambda: _run(
        scenarios.build_resolved([("s1", "s2", 0, 0)], initial_inventory={"s1": 1})
    ),
    "scenario-03": lambda: _run(
        scenarios.build_resolved([("s1", "s2", 0, 2)], initial_inventory={"s1": 1})
    ),
    "scenario-04": lambda: _run(
        scenarios.build_resolved(
            [("s1", "s3", 0, 0)] * 2 + [("s2", "s1", 6, 7)],
            capacities={"s3": 1},
            initial_inventory={"s1": 2},
        )
    ),
    "scenario-05": lambda: _run(scenarios.overflow_delayed()),
    "scenario-06": lambda: _run(
        scenarios.build_resolved(
            [("s1", "s3", 0, 1), ("s2", "s1", 6, 7)],
            capacities={"s3": 0},
            initial_inventory={"s1": 1},
        )
    ),
    "scenario-07": lambda: _run(
        scenarios.build_resolved(
            [("s1", "s4", 0, 1), ("s4", "s3", 0, 2)],
            capacities={"s4": 0},
            initial_inventory={"s1": 1, "s4": 1},
        )
    ),
    "scenario-08": lambda: _run(scenarios.redirect_chain()),
    "scenario-09": lambda: _run(scenarios.network_full()),
    "scenario-10": lambda: _run_rebalancing(
        MORNING_TRIPS,
        initial_inventory={"s1": 5},
        stops=scenarios.scripted_stops(3, pickup_minute=10.0, dropoff_minute=50.0),
    ),
    "scenario-11": lambda: _run_rebalancing(
        MORNING_TRIPS, initial_inventory={"s1": 5}, stops=scenarios.scripted_stops(3)
    ),
    "scenario-12": lambda: _run_rebalancing(
        MORNING_TRIPS,
        capacities={"s2": 2},
        initial_inventory={"s1": 5},
        stops=scenarios.scripted_stops(3),
    ),
    "scenario-13": lambda: _run_rebalancing(
        [("s1", "s2", 2, 3)] * 3 + MORNING_TRIPS,
        initial_inventory={"s1": 5},
        stops=scenarios.scripted_stops(3, pickup_minute=70.0, dropoff_minute=110.0),
    ),
    "scenario-14": _run_nothing_to_move,
}


@functools.cache
def run_doc_scenario(name: str):
    """Build and run one document scenario once; the checks share the run."""
    return DOC_SCENARIOS[name]()


# ---------------------------------------------------------------------------
# Reading the document and formatting the journal the way it prints
# ---------------------------------------------------------------------------
def read_doc_table(anchor: str) -> pd.DataFrame:
    """Parse the markdown table that follows ``<!-- table:<anchor> -->``."""
    text = DOC_PATH.read_text(encoding="utf-8")
    marker = f"<!-- table:{anchor} -->"
    assert marker in text, f"docs/scenarios.md has no anchor {marker}"
    lines: list[str] = []
    for line in text.split(marker, 1)[1].splitlines():
        stripped = line.strip()
        if stripped.startswith("|"):
            lines.append(stripped)
        elif lines:
            break
    assert len(lines) >= 3, f"no table under {marker}"
    header = [cell.strip() for cell in lines[0].strip("|").split("|")]
    rows = [[cell.strip() for cell in line.strip("|").split("|")] for line in lines[2:]]
    return pd.DataFrame(rows, columns=header)


def journal_as_doc_strings(rows: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Format journal rows the way the document prints them (missing -> "NA")."""

    def _cell(value) -> str:
        if pd.isna(value):
            return "NA"
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)

    return pd.DataFrame({col: [_cell(v) for v in rows[col]] for col in columns})


def assert_matches_doc(anchor: str, rows: pd.DataFrame, columns: list[str]) -> None:
    """Compare journal rows with the document's table, cell by cell."""
    expected = read_doc_table(anchor)
    assert list(expected.columns) == columns, f"{anchor}: column mismatch"
    actual = journal_as_doc_strings(rows.reset_index(drop=True), columns)
    pd.testing.assert_frame_equal(actual, expected)


def redirected_flow(journal: pd.DataFrame) -> pd.DataFrame:
    """Rows of the one flow that bounced, in event order."""
    flow_ids = journal.loc[journal["event_type"] == "redirected", "flow_id"].unique()
    assert len(flow_ids) == 1, "expected exactly one redirected flow"
    return journal[journal["flow_id"] == flow_ids[0]].sort_values("event_id")


def rebalance_rows(journal: pd.DataFrame) -> pd.DataFrame:
    """Select the rebalance flows' rows, in the order the document prints them."""
    rows = journal[journal["flow_type"] == "rebalance"]
    return rows.sort_values(["step_id", "flow_id", "event_id"])


# ---------------------------------------------------------------------------
# One test per table in the document
# ---------------------------------------------------------------------------
def test_scenario_01_stockout():
    _resolved, journal, _state = run_doc_scenario("scenario-01")
    assert_matches_doc("scenario-01", journal, TRIP_COLUMNS)
    assert journal["flow_id"].isna().all()  # the demand never became a flow


def test_scenario_02_plain_trip_same_period():
    _resolved, journal, _state = run_doc_scenario("scenario-02")
    assert_matches_doc("scenario-02", journal, TRIP_COLUMNS)


def test_scenario_03_plain_trip_later_period():
    _resolved, journal, _state = run_doc_scenario("scenario-03")
    assert_matches_doc("scenario-03", journal, TRIP_COLUMNS)


def test_scenario_04_redirect_same_period():
    _resolved, journal, _state = run_doc_scenario("scenario-04")
    assert_matches_doc("scenario-04", redirected_flow(journal), TRIP_COLUMNS)


def test_scenario_05_redirect_new_arc_later():
    _resolved, journal, _state = run_doc_scenario("scenario-05")
    flow = redirected_flow(journal)
    assert_matches_doc("scenario-05", flow, TRIP_COLUMNS)
    # The delayed docking joins period 2's normal dock batch: same step as the
    # plain trip docking at s2 that period.
    batch_mates = journal[
        (journal["event_type"] == "arrived")
        & (journal["move_id"] == 0)
        & (journal["period_id"] == 2)
    ]
    assert int(flow["step_id"].iloc[-1]) == int(batch_mates["step_id"].iloc[0])


def test_scenario_06_redirect_first_arc_later():
    _resolved, journal, _state = run_doc_scenario("scenario-06")
    assert_matches_doc("scenario-06", redirected_flow(journal), TRIP_COLUMNS)


def test_scenario_07_redirect_both_arcs_later():
    _resolved, journal, _state = run_doc_scenario("scenario-07")
    flow = redirected_flow(journal)
    assert_matches_doc("scenario-07", flow, TRIP_COLUMNS)
    # The step_id gap the document points at: step 2 is another flow's docking.
    other = journal[(journal["step_id"] == 2) & (journal["flow_id"] != flow["flow_id"].iloc[0])]
    assert not other.empty


def test_scenario_08_redirect_chain():
    _resolved, journal, _state = run_doc_scenario("scenario-08")
    bounced = journal[journal["event_type"] == "redirected"]
    flow_id = bounced["flow_id"].iloc[0]
    assert (bounced["flow_id"] == flow_id).all() and len(bounced) == 2
    flow = journal[journal["flow_id"] == flow_id].sort_values("event_id")
    assert_matches_doc("scenario-08", flow, TRIP_COLUMNS)


def test_scenario_09_network_full():
    _resolved, journal, _state = run_doc_scenario("scenario-09")
    assert (journal["event_type"] != "arrived").all()  # nothing ever docked
    flow_id = journal.loc[journal["event_type"] == "lost", "flow_id"].iloc[0]
    flow = journal[journal["flow_id"] == flow_id].sort_values("event_id")
    assert_matches_doc("scenario-09", flow, TRIP_COLUMNS)


def test_scenario_10_truck_route_inside_one_period():
    _resolved, journal, _state = run_doc_scenario("scenario-10")
    assert_matches_doc("scenario-10", rebalance_rows(journal), REBALANCE_COLUMNS)


def test_scenario_11_truck_route_across_periods():
    _resolved, journal, _state = run_doc_scenario("scenario-11")
    assert_matches_doc("scenario-11", rebalance_rows(journal), REBALANCE_COLUMNS)


def test_scenario_12_dropoff_overflow_docks_at_the_depot():
    _resolved, journal, _state = run_doc_scenario("scenario-12")
    assert_matches_doc("scenario-12", rebalance_rows(journal), REBALANCE_COLUMNS)
    # The document's consequence: one morning trip is lost to a stockout.
    lost = journal[(journal["event_type"] == "lost") & (journal["reason"] == "stockout")]
    assert lost["period_id"].tolist() == [6]
    assert int(lost["quantity"].sum()) == 1


def test_scenario_13_pickup_cut_to_the_bikes_on_hand():
    _resolved, journal, _state = run_doc_scenario("scenario-13")
    # The table has four rows: the third plan row was dropped with its dropoff.
    assert_matches_doc("scenario-13", rebalance_rows(journal), REBALANCE_COLUMNS)


def test_scenario_14_nothing_to_move():
    _resolved, journal, state = run_doc_scenario("scenario-14")
    assert journal[journal["flow_type"] == "rebalance"].empty
    assert state.rebalance_plan.empty


def test_every_doc_anchor_has_a_scenario():
    """Every table anchor in the document is backed by a scenario here."""
    text = DOC_PATH.read_text(encoding="utf-8")
    anchors = {part.split("-->", 1)[0].strip() for part in text.split("<!-- table:")[1:]}
    assert anchors <= set(DOC_SCENARIOS), f"unknown anchors: {anchors - set(DOC_SCENARIOS)}"


# ---------------------------------------------------------------------------
# The checks every scenario passes (the old "check it by eye" list, as code)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("name", sorted(DOC_SCENARIOS))
def test_journal_is_well_formed_and_run_invariants_hold(name):
    resolved, journal, state = run_doc_scenario(name)
    assert check_journal_well_formed(journal) == []
    assert validate_run(state, resolved) == []


@pytest.mark.parametrize("name", sorted(DOC_SCENARIOS))
def test_steps_apply_in_label_order(name):
    """One step, one label; steps sorted by step_id sort by their label too."""
    _resolved, journal, _state = run_doc_scenario(name)
    labels = journal[["step_id", "period_id", "phase_rank", "phase_round"]].drop_duplicates()
    assert labels["step_id"].is_unique
    ordered = labels.sort_values("step_id")
    tuples = list(
        zip(ordered["period_id"], ordered["phase_rank"], ordered["phase_round"], strict=True)
    )
    assert tuples == sorted(tuples)


@pytest.mark.parametrize("name", sorted(DOC_SCENARIOS))
def test_each_docked_flow_departs_once_and_docks_once(name):
    """A docked flow has one -1 and one +1, and departs before it docks."""
    _resolved, journal, _state = run_doc_scenario(name)
    flows = journal[journal["flow_id"].notna()]
    if flows.empty:
        return
    terminal = flows.sort_values("event_id").groupby("flow_id")["event_type"].last()
    docked_ids = terminal[terminal == "arrived"].index
    undock_counts = J.is_undocking(flows).groupby(flows["flow_id"]).sum()
    dock_counts = (flows["event_type"] == "arrived").groupby(flows["flow_id"]).sum()
    assert (undock_counts.loc[docked_ids] == 1).all()
    assert (dock_counts.loc[docked_ids] == 1).all()
    undock_step = flows[J.is_undocking(flows)].set_index("flow_id")["step_id"]
    dock_step = flows[flows["event_type"] == "arrived"].set_index("flow_id")["step_id"]
    assert (undock_step.loc[docked_ids] < dock_step.loc[docked_ids]).all()
