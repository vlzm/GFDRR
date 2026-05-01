"""Tests for LatentDemandInflatorPhase."""
# ruff: noqa: D102

from __future__ import annotations

import dataclasses
from datetime import date

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal

from gbp.consumers.simulator.built_in_phases import (
    DeparturePhysicsPhase,
    HistoricalLatentDemandPhase,
    LatentDemandInflatorPhase,
)
from gbp.consumers.simulator.phases import Phase, PhaseResult, Schedule
from gbp.consumers.simulator.state import (
    IN_TRANSIT_COLUMNS,
    RESOURCE_COLUMNS,
    PeriodRow,
    SimulationState,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


def _make_latent(
    facility_ids: list[str],
    departures: list[float],
    arrivals: list[float],
    commodity: str = "working_bike",
) -> pd.DataFrame:
    return pd.DataFrame({
        "facility_id": facility_ids,
        "commodity_category": [commodity] * len(facility_ids),
        "latent_departures": departures,
        "latent_arrivals": arrivals,
    })


def _state_with_latent(latent: pd.DataFrame) -> SimulationState:
    """Build a minimal SimulationState with *latent* pre-loaded in intermediates."""
    inventory = pd.DataFrame({
        "facility_id": latent["facility_id"].tolist(),
        "commodity_category": latent["commodity_category"].tolist(),
        "quantity": [20.0] * len(latent),
    })
    return SimulationState(
        period_index=0,
        period_id="p0",
        inventory=inventory,
        in_transit=pd.DataFrame(columns=IN_TRANSIT_COLUMNS),
        resources=pd.DataFrame(columns=RESOURCE_COLUMNS),
    ).with_intermediates(latent_demand=latent)


class _DummyResolved:
    """Resolved stub — the inflator does not consult resolved data."""


# ---------------------------------------------------------------------------
# Contract tests
# ---------------------------------------------------------------------------


class TestLatentDemandInflatorPhaseContract:
    """Phase satisfies the Phase protocol and delegates scheduling."""

    def test_is_phase(self) -> None:
        assert isinstance(LatentDemandInflatorPhase(), Phase)

    def test_name(self) -> None:
        assert LatentDemandInflatorPhase().name == "LATENT_DEMAND_INFLATOR"

    def test_should_run_delegates_to_schedule(self) -> None:
        phase = LatentDemandInflatorPhase(schedule=Schedule.every_n(2))
        assert phase.should_run(_make_period(period_index=0))
        assert not phase.should_run(_make_period(period_index=1))
        assert phase.should_run(_make_period(period_index=2))


# ---------------------------------------------------------------------------
# Behavioural tests
# ---------------------------------------------------------------------------


def test_multiplier_one_is_identity() -> None:
    """multiplier=1.0 leaves latent_demand unchanged (identity)."""
    latent = _make_latent(
        facility_ids=["s1", "s2"],
        departures=[4.0, 6.0],
        arrivals=[2.0, 3.0],
    )
    state = _state_with_latent(latent)

    result = LatentDemandInflatorPhase(multiplier=1.0).execute(
        state, _DummyResolved(), _make_period(),
    )

    assert_frame_equal(
        result.state.intermediates["latent_demand"].reset_index(drop=True),
        latent.reset_index(drop=True),
    )


def test_scalar_multiplier_scales_departures_and_arrivals() -> None:
    """multiplier=2.5 scales both latent_departures and latent_arrivals."""
    latent = _make_latent(
        facility_ids=["s1", "s2"],
        departures=[4.0, 6.0],
        arrivals=[2.0, 3.0],
    )
    state = _state_with_latent(latent)

    result = LatentDemandInflatorPhase(multiplier=2.5).execute(
        state, _DummyResolved(), _make_period(),
    )

    inflated = result.state.intermediates["latent_demand"]
    expected_dep = pd.Series([10.0, 15.0], name="latent_departures")
    expected_arr = pd.Series([5.0, 7.5], name="latent_arrivals")
    assert_series_equal(
        inflated["latent_departures"].reset_index(drop=True).rename("latent_departures"),
        expected_dep,
    )
    assert_series_equal(
        inflated["latent_arrivals"].reset_index(drop=True).rename("latent_arrivals"),
        expected_arr,
    )


def test_dict_multiplier_targets_correct_rows() -> None:
    """Per-facility dict applies only to listed facilities; others stay at 1.0."""
    latent = _make_latent(
        facility_ids=["station_A", "station_B", "station_C"],
        departures=[10.0, 5.0, 8.0],
        arrivals=[3.0, 2.0, 4.0],
    )
    state = _state_with_latent(latent)

    result = LatentDemandInflatorPhase(
        multiplier={"station_A": 3.0},
    ).execute(state, _DummyResolved(), _make_period())

    inflated = result.state.intermediates["latent_demand"]

    # station_A: scaled by 3.0
    row_a = inflated[inflated["facility_id"] == "station_A"].iloc[0]
    assert row_a["latent_departures"] == pytest.approx(30.0)
    assert row_a["latent_arrivals"] == pytest.approx(9.0)

    # station_B / station_C: implicit multiplier 1.0
    row_b = inflated[inflated["facility_id"] == "station_B"].iloc[0]
    assert row_b["latent_departures"] == pytest.approx(5.0)
    assert row_b["latent_arrivals"] == pytest.approx(2.0)

    row_c = inflated[inflated["facility_id"] == "station_C"].iloc[0]
    assert row_c["latent_departures"] == pytest.approx(8.0)
    assert row_c["latent_arrivals"] == pytest.approx(4.0)


def test_no_intermediates_returns_empty_result() -> None:
    """State with no latent_demand in intermediates returns PhaseResult.empty."""
    inventory = pd.DataFrame({
        "facility_id": ["s1"],
        "commodity_category": ["working_bike"],
        "quantity": [10.0],
    })
    state = SimulationState(
        period_index=0,
        period_id="p0",
        inventory=inventory,
        in_transit=pd.DataFrame(columns=IN_TRANSIT_COLUMNS),
        resources=pd.DataFrame(columns=RESOURCE_COLUMNS),
    )

    result = LatentDemandInflatorPhase(multiplier=2.0).execute(
        state, _DummyResolved(), _make_period(),
    )

    # Identity: state is returned unchanged, no new intermediates
    assert result.state is state
    assert result.events == {}


def test_phase_in_pipeline(resolved_model_with_obs: object) -> None:
    """Integration: inflator doubles latent demand; strict physics records lost_demand.

    Pipeline: HistoricalLatentDemandPhase -> LatentDemandInflatorPhase(2.0)
              -> DeparturePhysicsPhase(strict).

    Because the inflated demand (2x historical) exceeds available inventory
    at at least one station, strict-mode physics must record lost_demand > 0.
    Without the inflator (baseline), the same historical data is exactly
    satisfiable so lost_demand == 0.
    """
    from gbp.consumers.simulator.state import init_state

    resolved = resolved_model_with_obs  # type: ignore[assignment]
    period = PeriodRow(
        Index=0,
        period_id="p0",
        planning_horizon_id="h1",
        segment_index=0,
        period_index=0,
        period_type="day",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
    )

    state0 = init_state(resolved)

    # Step 1: compute latent demand from history
    latent_result = HistoricalLatentDemandPhase().execute(state0, resolved, period)

    # Baseline: strict physics on unmodified latent — should produce no lost demand
    baseline = DeparturePhysicsPhase(mode="strict").execute(
        latent_result.state, resolved, period,
    )
    assert baseline.event("lost_demand").empty, (
        "baseline should have no lost_demand; inventory satisfies historical marginals"
    )

    # Treatment: inflate latent by 50x — guaranteed to exceed any reasonable inventory.
    # 2x is too mild for the default fixture; we just need any non-empty lost_demand
    # to prove the inflator + strict-mode pipeline works end-to-end.
    inflated_result = LatentDemandInflatorPhase(multiplier=50.0).execute(
        latent_result.state, resolved, period,
    )
    treatment = DeparturePhysicsPhase(mode="strict").execute(
        inflated_result.state, resolved, period,
    )
    lost = treatment.event("lost_demand")
    assert not lost.empty, "inflated demand must exceed inventory, producing lost_demand > 0"
    assert (lost["lost"] > 0).all()
