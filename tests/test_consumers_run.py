"""Tests of ``gbp/consumers/run.py``'s ``RunRequest``, the one recipe every entry point builds."""

import pytest

from gbp.consumers.run import DEFAULT_TRUCK_HOMES, RunRequest


def test_forecast_source_requires_a_forecast_name():
    """A forecast run with no forecast name is rejected at construction."""
    with pytest.raises(ValueError, match="forecast_name"):
        RunRequest(run_name="bad", demand_source="forecast")


def test_history_source_needs_no_forecast_name():
    """A history run leaves ``forecast_name`` unset and validates."""
    request = RunRequest(run_name="ok")
    assert request.demand_source == "history"
    assert request.forecast_name is None


def test_resolved_truck_homes_falls_back_to_the_default_fleet():
    """With ``truck_homes`` unset the run uses the default fleet; a list is kept."""
    assert RunRequest(run_name="x").resolved_truck_homes() == DEFAULT_TRUCK_HOMES
    picked = RunRequest(run_name="x", truck_homes=["depot_2", "depot_3"])
    assert picked.resolved_truck_homes() == ["depot_2", "depot_3"]


def test_rebalancing_meta_off_and_on():
    """The meta block is ``enabled`` alone when off, and the fleet when on."""
    assert RunRequest(run_name="x").rebalancing_meta() == {"enabled": False}
    on = RunRequest(
        run_name="x", rebalancing=True, truck_homes=["depot_1"], truck_capacity_bikes=30
    )
    assert on.rebalancing_meta() == {
        "enabled": True,
        "truck_homes": ["depot_1"],
        "truck_capacity_bikes": 30,
    }
