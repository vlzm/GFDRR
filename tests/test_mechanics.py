"""Direct tests for the docking rule and the redirect planner.

``dock_up_to_capacity`` is the single "dock in row order up to free capacity"
rule for both docking sites -- arrivals at their planned station and a redirect
round at the station chosen for it -- so its tests cover both target columns
without a full engine run. The ``plan_overflow_redirect`` tests pin down where
a redirect leg's travel time comes from: the OD matrix when the pair has an
entry, the ``Routes`` estimate (distance over speed) when it does not.
"""

import pandas as pd

from gbp.consumers.simulator.mechanics import dock_up_to_capacity, plan_overflow_redirect
from gbp.routing import Routes

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


# ---------------------------------------------------------------------------
# Redirect leg travel time (``plan_overflow_redirect``)
# ---------------------------------------------------------------------------
def _redirect_setup(lat_b: float) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Station A is full (one dock, one bike); B, ``lat_b`` degrees north, is free."""
    inventory = pd.DataFrame(
        {"facility_id": ["A", "B"], "commodity_category": CLASSIC, "quantity": [1, 0]}
    )
    capacities = pd.DataFrame({"facility_id": ["A", "B"], "capacity": [1, 10]})
    geo = pd.DataFrame({"facility_id": ["A", "B"], "lat": [40.0, lat_b], "lng": [-74.0, -74.0]})
    overflow = pd.DataFrame({"planned_target_id": ["A"], "commodity_category": CLASSIC})
    return inventory, capacities, geo, overflow


def _routes(geo: pd.DataFrame, speed: float = 10.0) -> Routes:
    """Haversine-mode routes over the two test stations, at ``speed`` km per period."""
    return Routes(
        geo, "haversine", trip_speed_km_per_period=speed, period_len=pd.Timedelta(hours=1)
    )


def _od(rows: list[tuple[str, str, int]]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "source_id": [r[0] for r in rows],
            "planned_target_id": [r[1] for r in rows],
            "duration": [r[2] for r in rows],
        }
    )


def test_redirect_pair_missing_from_od_estimates_travel_time_from_distance():
    # No historical trip ever rode A -> B. B is 0.2 degrees north (~22.2 km);
    # at 10 km per period the leg takes round(2.22) = 2 periods.
    inventory, capacities, geo, overflow = _redirect_setup(lat_b=40.2)
    redirects, lost = plan_overflow_redirect(
        inventory, capacities, geo, _od([]), _routes(geo), overflow, period_id=5
    )
    assert lost.empty
    assert redirects["realized_target_id"].tolist() == ["B"]
    assert redirects["leg_end_period"].tolist() == [7]


def test_redirect_pair_present_in_od_keeps_the_historical_duration():
    # The OD matrix knows A -> B takes one period, so the distance estimate
    # (2 periods, as above) is not used.
    inventory, capacities, geo, overflow = _redirect_setup(lat_b=40.2)
    redirects, lost = plan_overflow_redirect(
        inventory, capacities, geo, _od([("A", "B", 1)]), _routes(geo), overflow, period_id=5
    )
    assert lost.empty
    assert redirects["leg_end_period"].tolist() == [6]


def test_redirect_to_a_nearby_station_still_docks_in_the_same_period():
    # B is ~0.11 km away; the estimate rounds to zero periods, so the bike
    # docks in the bounce period, as before the fallback existed.
    inventory, capacities, geo, overflow = _redirect_setup(lat_b=40.001)
    redirects, lost = plan_overflow_redirect(
        inventory, capacities, geo, _od([]), _routes(geo), overflow, period_id=5
    )
    assert lost.empty
    assert redirects["leg_end_period"].tolist() == [5]
