"""Unit tests for the flow-event builders and the structural checker.

These work directly on hand-built event frames -- no simulation run -- so they
pin down the *shape* of each event in isolation: the ``(move_id, event_id)`` an
event carries, and the four legal flow sequences. The second half deliberately
feeds the checker broken journals to prove it actually catches the mistakes it
claims to (a checker that never fails is worthless).
"""

import pandas as pd
import pytest

from gbp.model import journal as J
from gbp.model.journal import FLOW_EVENT_COLUMNS
from tests.invariants import check_journal_well_formed

CLASSIC = "classic_bike"


def _trip_frame() -> pd.DataFrame:
    """One trip A->B, departing period 5, due to dock period 7."""
    return pd.DataFrame({
        "flow_id":            ["sim_5_0"],
        "source_id":          ["A"],
        "planned_target_id":  ["B"],
        "commodity_category": [CLASSIC],
        "start_period":       [5],
        "planned_end_period": [7],
    })


# ---------------------------------------------------------------------------
# Each builder stamps the right (move_id, event_id)
# ---------------------------------------------------------------------------
def test_departed_is_move0_event0():
    row = J.departed_events(_trip_frame()).iloc[0]
    assert (row["move_id"], row["event_id"], row["event_type"]) == (0, 0, "departed")
    assert pd.isna(row["realized_target_id"])


def test_arrived_is_move0_event1_and_docks_at_planned_target():
    row = J.arrived_events(_trip_frame(), 7).iloc[0]
    assert (row["move_id"], row["event_id"], row["event_type"]) == (0, 1, "arrived")
    assert row["realized_target_id"] == "B"


def test_redirected_is_a_bounce_not_a_docking():
    row = J.redirected_events(_trip_frame(), 7).iloc[0]
    assert (row["move_id"], row["event_id"], row["event_type"]) == (0, 1, "redirected")
    assert row["reason"] == "dock_full"
    # The bounce docks nowhere -- that is the whole point of the redesign.
    assert pd.isna(row["realized_target_id"])
    assert "redirected" not in J.DOCKING_EVENT_TYPES


def test_redirect_continuation_is_the_second_arc():
    redirected = _trip_frame().assign(realized_target_id="C")
    cont = J.redirect_continuation_events(redirected, 7).sort_values("event_id")
    dep, arr = cont.iloc[0], cont.iloc[1]
    # Second-arc departure: B -> C, move 1, event 2, not a user departure.
    assert (dep["move_id"], dep["event_id"], dep["event_type"]) == (1, 2, "departed")
    assert dep["source_id"] == "B" and dep["planned_target_id"] == "C"
    assert pd.isna(dep["realized_target_id"])
    # Second-arc arrival: docks at C, move 1, event 3.
    assert (arr["move_id"], arr["event_id"], arr["event_type"]) == (1, 3, "arrived")
    assert arr["realized_target_id"] == "C"


@pytest.mark.parametrize("reason, expected_event_id", [("stockout", 0), ("dock_full", 1)])
def test_lost_event_id_follows_reason(reason, expected_event_id):
    losses = pd.DataFrame({
        "flow_id":            ["sim_5_0"] if reason == "dock_full" else [pd.NA],
        "source_id":          ["A"],
        "commodity_category": [CLASSIC],
        "quantity":           [1 if reason == "dock_full" else 3],
    })
    row = J.lost_events(losses, 7, reason).iloc[0]
    assert (row["move_id"], row["event_id"]) == (0, expected_event_id)
    assert pd.isna(row["realized_target_id"])


# ---------------------------------------------------------------------------
# A full redirect journal is well-formed; the four shapes are accepted
# ---------------------------------------------------------------------------
def test_full_redirect_journal_is_well_formed():
    trip = _trip_frame()
    redirected = trip.assign(realized_target_id="C")
    journal = J.finalize_flows(pd.concat([
        J.departed_events(trip),               # (0, 0)
        J.redirected_events(redirected, 7),    # (0, 1) bounce
        J.redirect_continuation_events(redirected, 7),  # (1, 2) + (1, 3)
    ], ignore_index=True))
    assert check_journal_well_formed(journal) == []


def test_empty_journal_is_well_formed():
    assert check_journal_well_formed(J.empty_flows_journal()) == []


# ---------------------------------------------------------------------------
# The checker must catch broken journals (testing the test)
# ---------------------------------------------------------------------------
def _good_normal_journal() -> pd.DataFrame:
    trip = _trip_frame()
    return J.finalize_flows(pd.concat(
        [J.departed_events(trip), J.arrived_events(trip, 7)], ignore_index=True
    ))


def test_checker_flags_duplicate_event_id():
    journal = _good_normal_journal()
    journal.loc[journal["event_type"] == "arrived", "event_id"] = 0  # collide with departed
    violations = check_journal_well_formed(journal)
    assert any("duplicate" in m for m in violations)


def test_checker_flags_a_redirect_with_no_continuation():
    # A bounce with no second arc -- the exact bug the redesign must avoid.
    trip = _trip_frame()
    journal = J.finalize_flows(pd.concat(
        [J.departed_events(trip), J.redirected_events(trip.assign(realized_target_id="C"), 7)],
        ignore_index=True,
    ))
    violations = check_journal_well_formed(journal)
    assert any("illegal event sequence" in m for m in violations)


def test_checker_flags_move_id_disagreeing_with_event_id():
    journal = _good_normal_journal()
    journal.loc[journal["event_type"] == "arrived", "move_id"] = 1  # event 1 must be arc 0
    violations = check_journal_well_formed(journal)
    assert any("move_id" in m for m in violations)


def test_checker_flags_time_running_backwards():
    journal = _good_normal_journal()
    # Force the arrival before the departure in time.
    journal.loc[journal["event_type"] == "arrived", "period_id"] = 0
    violations = check_journal_well_formed(journal)
    assert any("backwards" in m for m in violations)


def test_finalize_orders_events_within_a_flow():
    trip = _trip_frame()
    # Concatenate arrived before departed; finalize must still order 0 then 1.
    journal = J.finalize_flows(pd.concat(
        [J.arrived_events(trip, 7), J.departed_events(trip)], ignore_index=True
    ))
    assert journal["event_id"].tolist() == [0, 1]
    assert journal["event_type"].tolist() == ["departed", "arrived"]
    assert list(journal.columns) == FLOW_EVENT_COLUMNS
