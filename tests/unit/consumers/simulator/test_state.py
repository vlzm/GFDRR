"""Tests for SimulationState, init_state, and resource initialisation."""
# ruff: noqa: D102

from __future__ import annotations

import pandas as pd

from gbp.consumers.simulator.state import (
    IN_TRANSIT_COLUMNS,
    INVENTORY_COLUMNS,
    RESOURCE_COLUMNS,
    init_state,
)
from gbp.core.enums import ResourceStatus
from gbp.core.model import ResolvedModelData


class TestSimulationStateImmutability:
    """with_* helpers must return a new object, leaving the original intact."""

    def test_with_inventory_returns_new_state(
        self, resolved_model: ResolvedModelData
    ) -> None:
        state = init_state(resolved_model)
        new_inv = state.inventory.copy()
        new_inv["quantity"] = 0.0

        new_state = state.with_inventory(new_inv)

        assert new_state is not state
        assert new_state.inventory["quantity"].sum() == 0.0
        assert state.inventory["quantity"].sum() > 0.0

    def test_with_in_transit_returns_new_state(
        self, resolved_model: ResolvedModelData
    ) -> None:
        state = init_state(resolved_model)
        new_transit = pd.DataFrame(
            {
                "shipment_id": ["shp_001"],
                "source_id": ["d1"],
                "target_id": ["s1"],
                "commodity_category": ["working_bike"],
                "quantity": [5.0],
                "resource_id": ["truck_0"],
                "departure_period": [0],
                "arrival_period": [2],
            }
        )
        new_state = state.with_in_transit(new_transit)

        assert new_state is not state
        assert len(new_state.in_transit) == 1
        assert len(state.in_transit) == 0

    def test_with_resources_returns_new_state(
        self, resolved_model: ResolvedModelData
    ) -> None:
        state = init_state(resolved_model)
        new_res = state.resources.copy()
        new_res["status"] = ResourceStatus.MAINTENANCE.value

        new_state = state.with_resources(new_res)

        assert new_state is not state
        assert (new_state.resources["status"] == ResourceStatus.MAINTENANCE.value).all()
        assert (state.resources["status"] == ResourceStatus.AVAILABLE.value).all()

    def test_advance_period(self, resolved_model: ResolvedModelData) -> None:
        state = init_state(resolved_model)
        advanced = state.advance_period(next_period_index=1, next_period_id="p1")

        assert advanced.period_index == 1
        assert advanced.period_id == "p1"
        assert state.period_index == 0
        assert state.period_id == "p0"
        # DataFrames stay the same object (no copy needed for advance)
        assert advanced.inventory is state.inventory


class TestIntermediates:
    """``intermediates`` is a per-period scratch space cleared on advance."""

    def test_default_is_empty(self, resolved_model: ResolvedModelData) -> None:
        state = init_state(resolved_model)
        assert state.intermediates == {}

    def test_with_intermediates_merges(
        self, resolved_model: ResolvedModelData
    ) -> None:
        state = init_state(resolved_model)
        first = state.with_intermediates(latent_demand=pd.DataFrame({"x": [1]}))
        second = first.with_intermediates(od_probabilities=pd.DataFrame({"y": [2]}))

        assert set(second.intermediates.keys()) == {"latent_demand", "od_probabilities"}
        assert set(first.intermediates.keys()) == {"latent_demand"}
        assert state.intermediates == {}

    def test_advance_period_clears_intermediates(
        self, resolved_model: ResolvedModelData
    ) -> None:
        state = init_state(resolved_model).with_intermediates(latent_demand="anything")
        advanced = state.advance_period(next_period_index=1, next_period_id="p1")

        assert advanced.intermediates == {}
        # Original state is untouched.
        assert state.intermediates == {"latent_demand": "anything"}


class TestInitState:
    """init_state must produce correctly shaped DataFrames from resolved data."""

    def test_inventory_shape(self, resolved_model: ResolvedModelData) -> None:
        state = init_state(resolved_model)

        assert list(state.inventory.columns) == INVENTORY_COLUMNS
        # 3 facilities x 1 commodity = 3 rows
        assert len(state.inventory) == 3
        assert state.inventory["quantity"].sum() == 50.0 + 12.0 + 7.0

    def test_in_transit_empty(self, resolved_model: ResolvedModelData) -> None:
        state = init_state(resolved_model)

        assert list(state.in_transit.columns) == IN_TRANSIT_COLUMNS
        assert len(state.in_transit) == 0

    def test_first_period(self, resolved_model: ResolvedModelData) -> None:
        state = init_state(resolved_model)

        assert state.period_index == 0
        assert state.period_id == "p0"

    def test_resources_from_fleet(self, resolved_model: ResolvedModelData) -> None:
        """resource_fleet has 3 trucks at d1 -> 3 resource instances."""
        state = init_state(resolved_model)

        assert list(state.resources.columns) == RESOURCE_COLUMNS
        assert len(state.resources) == 3
        assert (state.resources["status"] == ResourceStatus.AVAILABLE.value).all()
        assert (state.resources["home_facility_id"] == "d1").all()
        assert (state.resources["current_facility_id"] == "d1").all()
        assert state.resources["available_at_period"].isna().all()
        # Each resource has a unique ID
        assert state.resources["resource_id"].nunique() == 3
