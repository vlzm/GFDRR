"""Tests of ``app/runner.py``'s ``run_scenario`` wiring.

The heavy pieces (forecast loading, the fleet swap, the sized run, the save)
are replaced by fakes that record what data object each one received, so the
tests check the wiring between the stages without a trip CSV.
"""

import pathlib
import sys

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "app"))

import runner  # noqa: E402  (needs the app folder on sys.path)


def test_forecast_run_with_rebalancing_keeps_the_forecast(monkeypatch):
    """The truck fleet is applied on top of the forecast data, not instead of it.

    Regression test: with ``demand_source="forecast"`` and ``rebalancing=True``,
    ``apply_truck_fleet`` used to receive the original ``graph_data``, silently
    dropping the applied forecast from the run.
    """
    seen = {}

    monkeypatch.setattr(runner.forecast, "load_forecast", lambda name: ("demand-table", "meta"))
    monkeypatch.setattr(runner.forecast, "forecast_periods_from_meta", lambda meta: "grid")
    monkeypatch.setattr(runner, "apply_forecast_demand", lambda data, demand, grid: "forecast-data")

    def fake_apply_truck_fleet(data, homes, capacity, rate):
        seen["fleet_input"] = data
        return "fleet-data"

    def fake_run_sized_scenario(data, **kwargs):
        seen["run_input"] = data
        return "result"

    def fake_save_scenario_run(result, data, **kwargs):
        seen["save_input"] = data
        return pathlib.Path("saved")

    monkeypatch.setattr(runner, "apply_truck_fleet", fake_apply_truck_fleet)
    monkeypatch.setattr(runner, "run_sized_scenario", fake_run_sized_scenario)
    monkeypatch.setattr(runner.artifacts, "save_scenario_run", fake_save_scenario_run)

    runner.run_scenario(
        "graph-data",
        run_name="forecast_with_trucks",
        demand_scale_factor=1.0,
        demand_source="forecast",
        forecast_name="some_forecast",
        rebalancing=True,
    )

    assert seen["fleet_input"] == "forecast-data"
    assert seen["run_input"] == "fleet-data"
    assert seen["save_input"] == "fleet-data"
