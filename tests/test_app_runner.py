"""Tests of ``app/runner.py``'s ``RunRequest`` and its ``run_scenario`` wiring.

The heavy pieces (forecast loading, the fleet swap, the sized run, the save)
are replaced by fakes that record what data object each one received, so the
tests check the wiring between the stages without a trip CSV.
"""

import pathlib
import sys

import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "app"))

import runner  # noqa: E402  (needs the app folder on sys.path)


# ---------------------------------------------------------------------------
# RunRequest: the one recipe every entry point builds
# ---------------------------------------------------------------------------
def test_forecast_source_requires_a_forecast_name():
    """A forecast run with no forecast name is rejected at construction."""
    with pytest.raises(ValueError, match="forecast_name"):
        runner.RunRequest(run_name="bad", demand_source="forecast")


def test_history_source_needs_no_forecast_name():
    """A history run leaves ``forecast_name`` unset and validates."""
    request = runner.RunRequest(run_name="ok")
    assert request.demand_source == "history"
    assert request.forecast_name is None


def test_resolved_truck_homes_falls_back_to_the_default_fleet():
    """With ``truck_homes`` unset the run uses the default fleet; a list is kept."""
    assert runner.RunRequest(run_name="x").resolved_truck_homes() == runner.DEFAULT_TRUCK_HOMES
    picked = runner.RunRequest(run_name="x", truck_homes=["depot_2", "depot_3"])
    assert picked.resolved_truck_homes() == ["depot_2", "depot_3"]


def test_rebalancing_meta_off_and_on():
    """The meta block is ``enabled`` alone when off, and the fleet when on."""
    assert runner.RunRequest(run_name="x").rebalancing_meta() == {"enabled": False}
    on = runner.RunRequest(
        run_name="x", rebalancing=True, truck_homes=["depot_1"], truck_capacity_bikes=30
    )
    assert on.rebalancing_meta() == {
        "enabled": True,
        "truck_homes": ["depot_1"],
        "truck_capacity_bikes": 30,
    }


# ---------------------------------------------------------------------------
# run_scenario: the stage wiring
# ---------------------------------------------------------------------------
def test_forecast_run_with_rebalancing_keeps_the_forecast(monkeypatch):
    """The truck fleet is applied on top of the forecast data, not instead of it.

    Regression test: with ``demand_source="forecast"`` and ``rebalancing=True``,
    ``apply_truck_fleet`` used to receive the original ``graph_data``, silently
    dropping the applied forecast from the run.
    """
    seen = {}

    monkeypatch.setattr(runner, "apply_saved_forecast", lambda data, name: ("forecast-data", 0.0))

    def fake_apply_truck_fleet(data, homes, capacity, rate):
        seen["fleet_input"] = data
        return "fleet-data"

    def fake_run_sized_scenario(data, **kwargs):
        seen["run_input"] = data
        return "result"

    def fake_save_scenario_run(result, data, request, **kwargs):
        seen["save_input"] = data
        return pathlib.Path("saved")

    monkeypatch.setattr(runner, "apply_truck_fleet", fake_apply_truck_fleet)
    monkeypatch.setattr(runner, "run_sized_scenario", fake_run_sized_scenario)
    monkeypatch.setattr(runner.artifacts, "save_scenario_run", fake_save_scenario_run)

    runner.run_scenario(
        "graph-data",
        runner.RunRequest(
            run_name="forecast_with_trucks",
            demand_source="forecast",
            forecast_name="some_forecast",
            rebalancing=True,
        ),
    )

    assert seen["fleet_input"] == "forecast-data"
    assert seen["run_input"] == "fleet-data"
    assert seen["save_input"] == "fleet-data"


def test_forecast_run_records_the_dropped_share(monkeypatch):
    """The dropped share ``apply_saved_forecast`` reports lands in the run's meta.

    The forecast step itself (load the artifact, cut the demand to the
    scenario, put it in place of the historical demand) lives in
    ``gbp.loaders.dataloader_graph.apply_saved_forecast`` and is tested
    there; the runner's job is to hand over the scenario data and the
    forecast name, run on what comes back, and record the reported share.
    """
    seen = {}

    def fake_apply_saved_forecast(data, name):
        seen["forecast_input"] = (data, name)
        return "forecast-data", 0.25

    def fake_run_sized_scenario(data, **kwargs):
        seen["run_input"] = data
        return "result"

    def fake_save_scenario_run(result, data, request, **kwargs):
        seen["save_kwargs"] = kwargs
        return pathlib.Path("saved")

    monkeypatch.setattr(runner, "apply_saved_forecast", fake_apply_saved_forecast)
    monkeypatch.setattr(runner, "run_sized_scenario", fake_run_sized_scenario)
    monkeypatch.setattr(runner.artifacts, "save_scenario_run", fake_save_scenario_run)

    runner.run_scenario(
        "graph-data",
        runner.RunRequest(
            run_name="forecast_cut",
            demand_source="forecast",
            forecast_name="some_forecast",
        ),
    )

    assert seen["forecast_input"] == ("graph-data", "some_forecast")
    assert seen["run_input"] == "forecast-data"
    assert seen["save_kwargs"]["forecast_dropped_share"] == 0.25


def test_history_run_saves_no_dropped_share(monkeypatch):
    """A history run records ``forecast_dropped_share=None`` — nothing was cut."""
    seen = {}

    def fake_save_scenario_run(result, data, request, **kwargs):
        seen["save_kwargs"] = kwargs
        return pathlib.Path("saved")

    monkeypatch.setattr(runner, "run_sized_scenario", lambda data, **kwargs: "result")
    monkeypatch.setattr(runner.artifacts, "save_scenario_run", fake_save_scenario_run)

    runner.run_scenario("graph-data", runner.RunRequest(run_name="history_run"))

    assert seen["save_kwargs"]["forecast_dropped_share"] is None
