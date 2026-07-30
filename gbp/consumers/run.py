"""The run path: the run recipe, and configure → run → save."""

from __future__ import annotations

import pathlib
from collections.abc import Callable
from typing import Literal

import pydantic

from gbp import artifacts
from gbp.consumers.simulator import (
    RebalancingParams,
    canonical_phases,
    rebalancing_phases,
    run_sized_scenario,
)
from gbp.ml.artifact import apply_saved_forecast
from gbp.model.dataloader_graph import ResolvedModelData

DEFAULT_NUMBER_OF_PERIODS = 50

#: Where a run's demand table can come from (Notations.md §11).
DEMAND_SOURCES = ("history", "forecast")


class RunRequest(pydantic.BaseModel):
    """The full recipe of one run: every parameter that says what to run.

    Framework parameters only. A domain that needs more -- its own data
    source, a fleet of resources -- subclasses this and adds its fields; see
    ``domains/citybike/run.py``.
    """

    run_name: str = pydantic.Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    demand_scale_factor: float = 1.0
    sizing_scale_factor: float = 1.0
    number_of_periods: int = DEFAULT_NUMBER_OF_PERIODS
    demand_source: Literal["history", "forecast"] = "history"
    #: A saved forecast (``data/ml/forecasts/``); required when
    #: ``demand_source="forecast"``.
    forecast_name: str | None = None
    rebalancing: bool = False

    @pydantic.model_validator(mode="after")
    def _forecast_needs_a_name(self) -> RunRequest:
        """Require a forecast name when the demand source is a forecast."""
        if self.demand_source == "forecast" and not self.forecast_name:
            raise ValueError("demand_source='forecast' needs a forecast_name")
        return self

    def rebalancing_meta(self) -> dict[str, object]:
        """Build the run's rebalancing block for ``meta.json``.

        A domain subclass overrides this to record its own fleet parameters:
        ``RebalancingMeta`` keeps every extra key it returns.
        """
        return {"enabled": self.rebalancing}


def run_scenario(
    graph_data: ResolvedModelData,
    request: RunRequest,
    *,
    root: pathlib.Path | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> pathlib.Path:
    """Resolve the run's demand from ``graph_data``, then run and save it."""

    def progress(message: str) -> None:
        if on_progress is not None:
            on_progress(message)

    data: ResolvedModelData = graph_data
    forecast_dropped_share: float | None = None
    if request.demand_source == "forecast":
        assert request.forecast_name is not None  # RunRequest guarantees this
        progress(
            f"Loading forecast {request.forecast_name} and mapping the OD matrix onto its horizon"
        )
        data, forecast_dropped_share = apply_saved_forecast(data, request.forecast_name)
        if forecast_dropped_share > 0:
            progress(
                f"Cut {forecast_dropped_share:.2%} of the forecast demand: rows the "
                "scenario has no OD rows for (unknown facilities or facility-hours)"
            )

    return run_and_save(
        data,
        request,
        forecast_dropped_share=forecast_dropped_share,
        root=root,
        on_progress=on_progress,
    )


def run_and_save(
    data: ResolvedModelData,
    request: RunRequest,
    *,
    sizing_data: ResolvedModelData | None = None,
    forecast_dropped_share: float | None = None,
    root: pathlib.Path | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> pathlib.Path:
    """Size and run the scenario on ``data``, then save the run artifact.

    ``data`` arrives ready: whatever resources the run uses are already on it
    (a domain puts its fleet there before calling), so this step only picks
    the phases the request asks for and runs them.
    """

    def progress(message: str) -> None:
        if on_progress is not None:
            on_progress(message)

    phases = None
    if request.rebalancing:
        phases = canonical_phases() + rebalancing_phases(RebalancingParams())

    progress("Sizing the state, running the simulation, checking the invariants I1-I5")
    result = run_sized_scenario(
        data,
        scenario_id=request.run_name,
        demand_scale_factor=request.demand_scale_factor,
        sizing_scale_factor=request.sizing_scale_factor,
        number_of_periods=request.number_of_periods,
        phases=phases,
        validate=False,
        sizing_data=sizing_data,
    )

    progress("Building and saving the run artifact")
    return artifacts.save_scenario_run(
        result,
        data,
        request,
        forecast_dropped_share=forecast_dropped_share,
        root=root,
    )
