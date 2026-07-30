"""Tests of ``gbp/consumers/run.py``: the run recipe and the ``run_scenario`` wiring.

The recipe tests pin ``RunRequest``. The wiring tests replace the heavy pieces
(forecast loading, the sized run, the save) with fakes that record what data
object each stage received, so they check the wiring between the stages
without a trip CSV. The Citi Bike half of the run path -- the truck fleet and
``build_graph_data`` -- is tested in ``tests/test_citybike_run.py``.
"""

import pathlib

import pytest

from gbp.consumers import run
from gbp.consumers.run import RunRequest


# ---------------------------------------------------------------------------
# RunRequest: the one recipe every entry point builds
# ---------------------------------------------------------------------------
def test_forecast_source_requires_a_forecast_name():
    """A forecast run with no forecast name is rejected at construction."""
    with pytest.raises(ValueError, match="forecast_name"):
        RunRequest(run_name="bad", demand_source="forecast")


def test_history_source_needs_no_forecast_name():
    """A history run leaves ``forecast_name`` unset and validates."""
    request = RunRequest(run_name="ok")
    assert request.demand_source == "history"
    assert request.forecast_name is None


def test_rebalancing_meta_is_the_flag_alone():
    """The framework block says whether rebalancing ran; a domain adds its fleet keys."""
    assert RunRequest(run_name="x").rebalancing_meta() == {"enabled": False}
    assert RunRequest(run_name="x", rebalancing=True).rebalancing_meta() == {"enabled": True}


def test_the_recipe_has_no_domain_parameters():
    """``RunRequest`` names no fleet: the truck fields belong to the Citi Bike subclass."""
    assert "truck_homes" not in RunRequest.model_fields
    assert "truck_capacity_bikes" not in RunRequest.model_fields


# ---------------------------------------------------------------------------
# run_scenario: the stage wiring
# ---------------------------------------------------------------------------
def test_rebalancing_run_uses_the_rebalancing_phases(monkeypatch):
    """With ``rebalancing=True`` the run adds the rebalancing phases to the canonical ones."""
    seen = {}

    def fake_run_sized_scenario(data, **kwargs):
        seen["phases"] = kwargs["phases"]
        return "result"

    monkeypatch.setattr(run, "run_sized_scenario", fake_run_sized_scenario)
    monkeypatch.setattr(
        run.artifacts, "save_scenario_run", lambda *args, **kwargs: pathlib.Path("saved")
    )

    run.run_scenario("graph-data", RunRequest(run_name="plain"))
    assert seen["phases"] is None

    run.run_scenario("graph-data", RunRequest(run_name="with_rebalancing", rebalancing=True))
    expected = run.canonical_phases() + run.rebalancing_phases(run.RebalancingParams())
    assert [type(phase).__name__ for phase in seen["phases"]] == [
        type(phase).__name__ for phase in expected
    ]


def test_forecast_run_records_the_dropped_share(monkeypatch):
    """The dropped share ``apply_saved_forecast`` reports lands in the run's meta.

    The forecast step itself (load the artifact, cut the demand to the
    scenario, put it in place of the historical demand) lives in
    ``gbp.model.dataloader_graph.apply_saved_forecast`` and is tested
    there; the run path's job is to hand over the scenario data and the
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

    monkeypatch.setattr(run, "apply_saved_forecast", fake_apply_saved_forecast)
    monkeypatch.setattr(run, "run_sized_scenario", fake_run_sized_scenario)
    monkeypatch.setattr(run.artifacts, "save_scenario_run", fake_save_scenario_run)

    run.run_scenario(
        "graph-data",
        RunRequest(
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

    monkeypatch.setattr(run, "run_sized_scenario", lambda data, **kwargs: "result")
    monkeypatch.setattr(run.artifacts, "save_scenario_run", fake_save_scenario_run)

    run.run_scenario("graph-data", RunRequest(run_name="history_run"))

    assert seen["save_kwargs"]["forecast_dropped_share"] is None
