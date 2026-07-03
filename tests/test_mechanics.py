"""Direct tests for the docking rule (``dock_up_to_capacity``).

One function is now the single "dock in row order up to free capacity" rule for
both docking sites -- arrivals at their planned station and a redirect round at
the station chosen for it -- so these tests cover both target columns without a
full engine run.
"""

import pandas as pd

from gbp.consumers.simulator.mechanics import dock_up_to_capacity

CLASSIC = "classic_bike"


def _due(targets: list[str], target_col: str = "planned_target_id") -> pd.DataFrame:
    return pd.DataFrame({target_col: targets, "commodity_category": CLASSIC})


def test_docks_in_row_order_up_to_free_capacity():
    due = _due(["A", "A", "A", "B"])
    free = pd.Series({"A": 2, "B": 5})
    fits, overflow = dock_up_to_capacity(due, free)
    # The first two rows aiming at A fit; the third overflows; B has room.
    assert fits.index.tolist() == [0, 1, 3]
    assert overflow.index.tolist() == [2]


def test_a_target_without_a_free_docks_entry_takes_nothing():
    due = _due(["C"])
    free = pd.Series({"A": 2})
    fits, overflow = dock_up_to_capacity(due, free)
    assert fits.empty
    assert overflow.index.tolist() == [0]


def test_target_col_selects_the_station_column():
    # The redirect round docks at ``realized_target_id``: the planned station is
    # full, but the chosen one has a slot.
    due = pd.DataFrame(
        {
            "planned_target_id": ["A", "A"],
            "realized_target_id": ["B", "B"],
            "commodity_category": CLASSIC,
        }
    )
    free = pd.Series({"A": 0, "B": 1})
    fits, overflow = dock_up_to_capacity(due, free, "realized_target_id")
    assert fits.index.tolist() == [0]
    assert overflow.index.tolist() == [1]


def test_empty_input_returns_two_empty_frames():
    due = _due([])
    free = pd.Series(dtype="int64")
    fits, overflow = dock_up_to_capacity(due, free)
    assert fits.empty and overflow.empty
