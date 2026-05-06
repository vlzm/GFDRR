"""Tests for EndOfPeriodDeficitPhase."""
# ruff: noqa: D102

from __future__ import annotations

import dataclasses
from datetime import date

import pandas as pd
import pytest

from gbp.consumers.simulator.built_in_phases import (
    ArrivalsPhase,
    DeparturePhysicsPhase,
    EndOfPeriodDeficitPhase,
    HistoricalLatentDemandPhase,
    HistoricalODStructurePhase,
    HistoricalTripSamplingPhase,
    InvariantCheckPhase,
    LatentDemandInflatorPhase,
    OverflowRedirectPhase,
)
from gbp.consumers.simulator.config import EnvironmentConfig
from gbp.consumers.simulator.engine import Environment
from gbp.consumers.simulator.phases import Phase, Schedule
from gbp.consumers.simulator.state import (
    IN_TRANSIT_COLUMNS,
    RESOURCE_COLUMNS,
    PeriodRow,
    SimulationState,
    init_state,
)
from gbp.core.model import ResolvedModelData


def _make_period(period_index: int = 0, period_id: str = "p0") -> PeriodRow:
    return PeriodRow(
        Index=0,
        period_id=period_id,
        planning_horizon_id="h1",
        segment_index=0,
        period_index=period_index,
        period_type="day",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
    )


class _DummyResolved:
    """Resolved stub — the deficit phase does not consult resolved data."""


def _make_state(
    inventory: pd.DataFrame,
    in_transit: pd.DataFrame | None = None,
    period_index: int = 0,
) -> SimulationState:
    return SimulationState(
        period_index=period_index,
        period_id=f"p{period_index}",
        inventory=inventory,
        in_transit=(
            in_transit
            if in_transit is not None
            else pd.DataFrame(columns=IN_TRANSIT_COLUMNS)
        ),
        resources=pd.DataFrame(columns=RESOURCE_COLUMNS),
    )


# ---------------------------------------------------------------------------
# Contract
# ---------------------------------------------------------------------------


class TestEndOfPeriodDeficitPhaseContract:
    """Phase satisfies the Phase protocol and delegates scheduling."""

    def test_is_phase(self) -> None:
        assert isinstance(EndOfPeriodDeficitPhase(), Phase)

    def test_name(self) -> None:
        assert EndOfPeriodDeficitPhase().name == "END_OF_PERIOD_DEFICIT"

    def test_should_run_delegates_to_schedule(self) -> None:
        phase = EndOfPeriodDeficitPhase(schedule=Schedule.every_n(2))
        assert phase.should_run(_make_period(period_index=0))
        assert not phase.should_run(_make_period(period_index=1))
        assert phase.should_run(_make_period(period_index=2))


# ---------------------------------------------------------------------------
# Behavioural tests
# ---------------------------------------------------------------------------


def test_no_negative_inventory_is_noop() -> None:
    """When all inventory is non-negative, the phase returns an empty result."""
    inventory = pd.DataFrame({
        "facility_id": ["s1", "s2"],
        "commodity_category": ["working_bike"] * 2,
        "quantity": [5.0, 0.0],
    })
    state = _make_state(inventory)

    result = EndOfPeriodDeficitPhase().execute(
        state, _DummyResolved(), _make_period(),
    )

    assert result.events == {}
    pd.testing.assert_frame_equal(
        result.state.inventory.reset_index(drop=True),
        inventory.reset_index(drop=True),
    )


def test_negative_inventory_is_clipped_and_logged() -> None:
    """A row with negative quantity is clipped to zero and recorded."""
    inventory = pd.DataFrame({
        "facility_id": ["s1", "s2"],
        "commodity_category": ["working_bike"] * 2,
        "quantity": [-3.0, 4.0],
    })
    state = _make_state(inventory)

    result = EndOfPeriodDeficitPhase().execute(
        state, _DummyResolved(), _make_period(),
    )

    new_inv = result.state.inventory.set_index("facility_id")["quantity"]
    assert new_inv["s1"] == pytest.approx(0.0)
    assert new_inv["s2"] == pytest.approx(4.0)

    lost = result.events["lost_demand"]
    assert len(lost) == 1
    assert lost.iloc[0]["facility_id"] == "s1"
    assert lost.iloc[0]["lost"] == pytest.approx(3.0)


def test_tolerance_ignores_floating_point_noise() -> None:
    """Tiny negatives below tolerance are not treated as deficits."""
    inventory = pd.DataFrame({
        "facility_id": ["s1"],
        "commodity_category": ["working_bike"],
        "quantity": [-1e-12],
    })
    state = _make_state(inventory)

    result = EndOfPeriodDeficitPhase(tolerance=1e-9).execute(
        state, _DummyResolved(), _make_period(),
    )

    assert result.events == {}


def test_in_transit_reduced_to_cover_deficit() -> None:
    """Current-period shipments from the deficit facility are reduced."""
    inventory = pd.DataFrame({
        "facility_id": ["s1"],
        "commodity_category": ["working_bike"],
        "quantity": [-5.0],
    })
    in_transit = pd.DataFrame({
        "shipment_id": ["t1", "t2"],
        "source_id": ["s1", "s1"],
        "target_id": ["s2", "s3"],
        "commodity_category": ["working_bike", "working_bike"],
        "quantity": [3.0, 4.0],
        "resource_id": [None, None],
        "departure_period": [0, 0],
        "arrival_period": [1, 2],
    })
    state = _make_state(inventory, in_transit, period_index=0)

    result = EndOfPeriodDeficitPhase().execute(
        state, _DummyResolved(), _make_period(period_index=0),
    )

    # Deficit of 5 reduces t1 by 3 (now 0, dropped), then t2 by 2 (now 2 left).
    new_transit = result.state.in_transit
    assert len(new_transit) == 1
    assert new_transit.iloc[0]["shipment_id"] == "t2"
    assert new_transit.iloc[0]["quantity"] == pytest.approx(2.0)


def test_in_transit_only_current_period_shipments_reduced() -> None:
    """Older-period shipments are not touched (they are not phantoms)."""
    inventory = pd.DataFrame({
        "facility_id": ["s1"],
        "commodity_category": ["working_bike"],
        "quantity": [-2.0],
    })
    in_transit = pd.DataFrame({
        "shipment_id": ["older", "current"],
        "source_id": ["s1", "s1"],
        "target_id": ["s2", "s3"],
        "commodity_category": ["working_bike", "working_bike"],
        "quantity": [10.0, 5.0],
        "resource_id": [None, None],
        "departure_period": [0, 1],  # older=period 0, current=period 1
        "arrival_period": [2, 2],
    })
    state = _make_state(inventory, in_transit, period_index=1)

    result = EndOfPeriodDeficitPhase().execute(
        state, _DummyResolved(), _make_period(period_index=1),
    )

    # Older shipment (departure_period=0) is untouched; current (period 1)
    # is reduced by 2.
    by_id = result.state.in_transit.set_index("shipment_id")["quantity"]
    assert by_id["older"] == pytest.approx(10.0)
    assert by_id["current"] == pytest.approx(3.0)


def test_lost_log_uses_latent_intermediate_when_available() -> None:
    """When intermediates['latent_demand'] is set, latent/realized are populated."""
    inventory = pd.DataFrame({
        "facility_id": ["s1"],
        "commodity_category": ["working_bike"],
        "quantity": [-2.0],
    })
    latent = pd.DataFrame({
        "facility_id": ["s1"],
        "commodity_category": ["working_bike"],
        "latent_departures": [10.0],
        "latent_arrivals": [5.0],
    })
    state = _make_state(inventory).with_intermediates(latent_demand=latent)

    result = EndOfPeriodDeficitPhase().execute(
        state, _DummyResolved(), _make_period(),
    )

    lost = result.events["lost_demand"].iloc[0]
    assert lost["latent"] == pytest.approx(10.0)
    assert lost["realized"] == pytest.approx(8.0)
    assert lost["lost"] == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# Integration with the historical-replay pipeline
# ---------------------------------------------------------------------------


def _baseline_per_commodity(state: SimulationState) -> dict[str, float]:
    if state.inventory.empty:
        inv_totals = pd.Series(dtype=float)
    else:
        inv_totals = state.inventory.groupby(
            "commodity_category",
        )["quantity"].sum()
    if state.in_transit.empty:
        transit_totals = pd.Series(dtype=float)
    else:
        transit_totals = state.in_transit.groupby(
            "commodity_category",
        )["quantity"].sum()
    combined = inv_totals.add(transit_totals, fill_value=0.0)
    return {str(k): float(v) for k, v in combined.items()}


def _replay_phases(extra: list[Phase] | None = None) -> list[Phase]:
    return [
        HistoricalLatentDemandPhase(),
        HistoricalODStructurePhase(),
        *(extra or []),
        DeparturePhysicsPhase(mode="permissive"),
        HistoricalTripSamplingPhase(use_durations=True),
        ArrivalsPhase(),
        OverflowRedirectPhase(),
        EndOfPeriodDeficitPhase(),
    ]


def test_canonical_replay_no_lost_demand(
    resolved_model_with_obs: ResolvedModelData,
) -> None:
    """multiplier=1.0 historical replay produces zero lost_demand at every period."""
    resolved = dataclasses.replace(resolved_model_with_obs, supply=None)
    state = init_state(resolved)
    baseline = _baseline_per_commodity(state)

    phases = [
        *_replay_phases(),
        InvariantCheckPhase(baseline=baseline, fail_on_violation=True),
    ]
    env = Environment(
        resolved,
        EnvironmentConfig(
            phases=phases, seed=42, scenario_id="end_of_period_canonical",
        ),
    )
    log = env.run()
    tables = log.to_dataframes()

    assert tables["simulation_lost_demand_log"].empty
    assert tables["simulation_invariant_violation_log"].empty


def test_inflated_replay_produces_lost_demand(
    resolved_model_with_obs: ResolvedModelData,
) -> None:
    """With a 50x inflator, the end-of-period clip records lost_demand > 0."""
    resolved = dataclasses.replace(resolved_model_with_obs, supply=None)

    phases = _replay_phases(
        extra=[LatentDemandInflatorPhase(multiplier=50.0)],
    )
    # Drop the invariant check: the inflator itself breaks conservation,
    # which is orthogonal to what this test validates.
    env = Environment(
        resolved,
        EnvironmentConfig(
            phases=phases, seed=42, scenario_id="end_of_period_inflated",
        ),
    )
    log = env.run()
    tables = log.to_dataframes()

    lost = tables["simulation_lost_demand_log"]
    assert not lost.empty
    assert (lost["lost"] > 0).all()
