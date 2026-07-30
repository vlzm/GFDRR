"""Tests of ``domains/citybike/run.py``: the Citi Bike half of the run path.

The recipe tests pin the truck fields the domain adds to the framework's
``RunRequest``. The wiring test replaces every heavy stage (the fleet swap,
the forecast loading, the sized run, the save) with a fake that records what
data object it received, so it checks the order of the stages without a trip
CSV.
"""

import pathlib

from domains.citybike import run
from domains.citybike.run import DEFAULT_TRUCK_HOMES, RunRequest
from gbp.consumers import run as gbp_run


# ---------------------------------------------------------------------------
# The recipe: the framework's fields plus the truck fleet
# ---------------------------------------------------------------------------
def test_resolved_truck_homes_falls_back_to_the_default_fleet():
    """With ``truck_homes`` unset the run uses the default fleet; a list is kept."""
    assert RunRequest(run_name="x").resolved_truck_homes() == DEFAULT_TRUCK_HOMES
    picked = RunRequest(run_name="x", truck_homes=["depot_2", "depot_3"])
    assert picked.resolved_truck_homes() == ["depot_2", "depot_3"]


def test_rebalancing_meta_off_and_on():
    """The meta block is ``enabled`` alone when off, and the fleet on top when on."""
    assert RunRequest(run_name="x").rebalancing_meta() == {"enabled": False}
    on = RunRequest(
        run_name="x", rebalancing=True, truck_homes=["depot_1"], truck_capacity_bikes=30
    )
    assert on.rebalancing_meta() == {
        "enabled": True,
        "truck_homes": ["depot_1"],
        "truck_capacity_bikes": 30,
    }


def test_the_recipe_still_validates_the_forecast_name():
    """The domain recipe inherits the framework's rules, not just its fields."""
    request = RunRequest(run_name="ok", demand_source="forecast", forecast_name="fc")
    assert request.forecast_name == "fc"
    assert isinstance(request, gbp_run.RunRequest)


# ---------------------------------------------------------------------------
# run_scenario: the fleet goes on first, the framework takes it from there
# ---------------------------------------------------------------------------
def test_forecast_run_with_rebalancing_keeps_both(monkeypatch):
    """The fleet and the forecast both reach the run, in that order.

    The domain puts the fleet on the scenario data, then hands it to the
    framework, which substitutes the forecast on top. Both stages copy the
    container and write different attributes, so the run must see the data
    that went through both.
    """
    seen = {}

    def fake_apply_truck_fleet(data, homes, capacity, rate):
        seen["fleet_input"] = data
        seen["fleet_homes"] = homes
        return "fleet-data"

    def fake_apply_saved_forecast(data, name):
        seen["forecast_input"] = data
        return "forecast-of-fleet-data", 0.0

    def fake_run_sized_scenario(data, **kwargs):
        seen["run_input"] = data
        return "result"

    def fake_save_scenario_run(result, data, request, **kwargs):
        seen["save_input"] = data
        return pathlib.Path("saved")

    monkeypatch.setattr(run, "apply_truck_fleet", fake_apply_truck_fleet)
    monkeypatch.setattr(gbp_run, "apply_saved_forecast", fake_apply_saved_forecast)
    monkeypatch.setattr(gbp_run, "run_sized_scenario", fake_run_sized_scenario)
    monkeypatch.setattr(gbp_run.artifacts, "save_scenario_run", fake_save_scenario_run)

    run.run_scenario(
        "graph-data",
        RunRequest(
            run_name="forecast_with_trucks",
            demand_source="forecast",
            forecast_name="some_forecast",
            rebalancing=True,
            truck_homes=["depot_1", "depot_2"],
        ),
    )

    assert seen["fleet_input"] == "graph-data"
    assert seen["fleet_homes"] == ["depot_1", "depot_2"]
    assert seen["forecast_input"] == "fleet-data"
    assert seen["run_input"] == "forecast-of-fleet-data"
    assert seen["save_input"] == "forecast-of-fleet-data"


def test_a_run_without_rebalancing_touches_no_fleet(monkeypatch):
    """With rebalancing off the scenario data goes to the framework untouched."""
    seen = {}

    def fail_apply_truck_fleet(*args, **kwargs):
        raise AssertionError("the fleet must not be applied when rebalancing is off")

    monkeypatch.setattr(run, "apply_truck_fleet", fail_apply_truck_fleet)
    monkeypatch.setattr(
        gbp_run, "run_sized_scenario", lambda data, **kwargs: seen.setdefault("run_input", data)
    )
    monkeypatch.setattr(
        gbp_run.artifacts, "save_scenario_run", lambda *args, **kwargs: pathlib.Path("saved")
    )

    run.run_scenario("graph-data", RunRequest(run_name="plain"))
    assert seen["run_input"] == "graph-data"
