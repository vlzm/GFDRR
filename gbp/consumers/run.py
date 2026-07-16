"""The run recipe and the fleet defaults every entry point shares."""

from __future__ import annotations

from typing import Literal

import pydantic

DEFAULT_NUMBER_OF_PERIODS = 50

#: Where a run's demand table can come from (Notations.md §11).
DEMAND_SOURCES = ("history", "forecast")

# The synthetic depot and truck fleet (see gbp/loaders/dataloader_raw.py).
DEFAULT_N_DEPOTS = 10
DEPOT_IDS = [f"depot_{i + 1}" for i in range(DEFAULT_N_DEPOTS)]
DEFAULT_TRUCK_HOMES = ["depot_1"] * 5
DEFAULT_TRUCK_CAPACITY_BIKES = 20
DEFAULT_TRUCK_RATE = 50.0


class RunRequest(pydantic.BaseModel):
    """The full recipe of one run: every parameter that says what to run."""

    run_name: str = pydantic.Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    demand_scale_factor: float = 1.0
    sizing_scale_factor: float = 1.0
    number_of_periods: int = DEFAULT_NUMBER_OF_PERIODS
    demand_source: Literal["history", "forecast"] = "history"
    #: A saved forecast (``data/ml/forecasts/``); required when
    #: ``demand_source="forecast"``.
    forecast_name: str | None = None
    rebalancing: bool = False
    truck_homes: list[str] | None = None
    truck_capacity_bikes: int = DEFAULT_TRUCK_CAPACITY_BIKES

    @pydantic.model_validator(mode="after")
    def _forecast_needs_a_name(self) -> RunRequest:
        """Require a forecast name when the demand source is a forecast."""
        if self.demand_source == "forecast" and not self.forecast_name:
            raise ValueError("demand_source='forecast' needs a forecast_name")
        return self

    def resolved_truck_homes(self) -> list[str]:
        """Home depot per truck the run uses: the request's list, or the default fleet."""
        if self.truck_homes is not None:
            return list(self.truck_homes)
        return list(DEFAULT_TRUCK_HOMES)

    def rebalancing_meta(self) -> dict[str, object]:
        """Build the run's rebalancing block for ``meta.json``."""
        meta: dict[str, object] = {"enabled": self.rebalancing}
        if self.rebalancing:
            meta["truck_homes"] = self.resolved_truck_homes()
            meta["truck_capacity_bikes"] = self.truck_capacity_bikes
        return meta
