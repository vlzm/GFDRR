"""Tests for HistoricalTripSamplingPhase τ_k handling."""

from __future__ import annotations

import dataclasses
from datetime import date

import numpy as np
import pandas as pd

from gbp.consumers.simulator.built_in_phases import HistoricalTripSamplingPhase
from gbp.consumers.simulator.state import (
    IN_TRANSIT_COLUMNS,
    INVENTORY_COLUMNS,
    PeriodRow,
    SimulationState,
)
from gbp.core.model import ResolvedModelData


def _empty_state(period_index: int = 0, period_id: str = "p0") -> SimulationState:
    """Return a SimulationState with empty inventory/in_transit."""
    return SimulationState(
        period_index=period_index,
        period_id=period_id,
        inventory=pd.DataFrame(columns=INVENTORY_COLUMNS),
        in_transit=pd.DataFrame(columns=IN_TRANSIT_COLUMNS),
        resources=pd.DataFrame(
            columns=[
                "resource_id", "resource_category",
                "home_facility_id", "current_facility_id",
                "status", "available_at_period",
            ],
        ),
    )


def _period_row(period_index: int = 0, period_id: str = "p0") -> PeriodRow:
    """Construct a PeriodRow matching the simulator state."""
    return PeriodRow(
        Index=period_index,
        period_id=period_id,
        planning_horizon_id="h1",
        segment_index=0,
        period_index=period_index,
        period_type="day",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
    )


def _replace_observed_flow(
    resolved: ResolvedModelData, observed_flow: pd.DataFrame,
) -> ResolvedModelData:
    """Return *resolved* with ``observed_flow`` swapped (light-touch helper)."""
    return dataclasses.replace(resolved, observed_flow=observed_flow)


def test_zero_tau_falls_back_to_same_period(
    resolved_model_with_obs: ResolvedModelData,
) -> None:
    """When duration_hours is null, arrival_period equals period_index."""
    obs = pd.DataFrame(
        {
            "source_id": ["s1"],
            "target_id": ["s2"],
            "commodity_category": ["working_bike"],
            "period_id": ["p0"],
            "quantity": [3.0],
            "duration_hours": [None],
            "modal_type": [None],
            "resource_id": [None],
        }
    )
    resolved = _replace_observed_flow(resolved_model_with_obs, obs)
    phase = HistoricalTripSamplingPhase(use_durations=True)
    state = _empty_state()
    result = phase.execute(state, resolved, _period_row())
    in_transit = result.state.in_transit
    assert (in_transit["arrival_period"] == 0).all()
    assert (in_transit["departure_period"] == 0).all()


def test_within_period_tau_same_period(
    resolved_model_with_obs: ResolvedModelData,
) -> None:
    """Durations smaller than one period stay in the same period.

    With period_duration_hours=24 and duration_hours in {0, 2, 23.99}, the
    floor-based formula yields tau_periods=0 for all -> arrival_period equals
    the current period_index.
    """
    obs = pd.DataFrame(
        {
            "source_id": ["s1", "s2", "s1"],
            "target_id": ["s2", "s1", "s2"],
            "commodity_category": ["working_bike"] * 3,
            "period_id": ["p0", "p0", "p0"],
            "quantity": [1.0, 1.0, 1.0],
            "duration_hours": [0.0, 2.0, 23.99],
            "modal_type": [None, None, None],
            "resource_id": [None, None, None],
        }
    )
    resolved = _replace_observed_flow(resolved_model_with_obs, obs)
    phase = HistoricalTripSamplingPhase(use_durations=True)
    result = phase.execute(_empty_state(), resolved, _period_row())
    arrivals = result.state.in_transit["arrival_period"].to_numpy()
    np.testing.assert_array_equal(arrivals, [0, 0, 0])


def test_cross_period_tau_advances_arrival(
    resolved_model_with_obs: ResolvedModelData,
) -> None:
    """Durations exceeding one period bump arrival_period via floor division."""
    obs = pd.DataFrame(
        {
            "source_id": ["s1", "s1", "s1"],
            "target_id": ["s2", "s2", "s2"],
            "commodity_category": ["working_bike"] * 3,
            "period_id": ["p0", "p0", "p0"],
            "quantity": [1.0, 1.0, 1.0],
            # floor(24/24)=1, floor(25/24)=1, floor(49/24)=2.
            "duration_hours": [24.0, 25.0, 49.0],
            "modal_type": [None, None, None],
            "resource_id": [None, None, None],
        }
    )
    resolved = _replace_observed_flow(resolved_model_with_obs, obs)
    phase = HistoricalTripSamplingPhase(use_durations=True)
    result = phase.execute(_empty_state(), resolved, _period_row())
    arrivals = result.state.in_transit["arrival_period"].to_numpy()
    np.testing.assert_array_equal(arrivals, [1, 1, 2])


def test_use_durations_false_legacy_path(
    resolved_model_with_obs: ResolvedModelData,
) -> None:
    """use_durations=False ignores duration_hours even when present."""
    obs = pd.DataFrame(
        {
            "source_id": ["s1"],
            "target_id": ["s2"],
            "commodity_category": ["working_bike"],
            "period_id": ["p0"],
            "quantity": [1.0],
            "duration_hours": [25.0],
            "modal_type": [None],
            "resource_id": [None],
        }
    )
    resolved = _replace_observed_flow(resolved_model_with_obs, obs)
    phase = HistoricalTripSamplingPhase(use_durations=False)
    result = phase.execute(_empty_state(), resolved, _period_row())
    arrivals = result.state.in_transit["arrival_period"].to_numpy()
    np.testing.assert_array_equal(arrivals, [0])


def test_empty_flow_period_is_noop(
    resolved_model_with_obs: ResolvedModelData,
) -> None:
    """Empty per-period observed_flow → no in_transit additions, no events."""
    obs = pd.DataFrame(
        columns=[
            "source_id", "target_id", "commodity_category",
            "period_id", "quantity", "duration_hours",
            "modal_type", "resource_id",
        ],
    )
    resolved = _replace_observed_flow(resolved_model_with_obs, obs)
    phase = HistoricalTripSamplingPhase(use_durations=True)
    state = _empty_state()
    result = phase.execute(state, resolved, _period_row())
    assert result.state.in_transit.empty
    assert result.events == {}


def test_no_duration_column_falls_back(
    resolved_model_with_obs: ResolvedModelData,
) -> None:
    """Legacy observed_flow without duration_hours column → same-period τ=0.

    Mirrors the pre-extension fixture path: callers that have not yet
    rebuilt their data with the new column still get the legacy behaviour.
    """
    obs = pd.DataFrame(
        {
            "source_id": ["s1"],
            "target_id": ["s2"],
            "commodity_category": ["working_bike"],
            "period_id": ["p0"],
            "quantity": [2.0],
            "modal_type": [None],
            "resource_id": [None],
        }
    )
    resolved = _replace_observed_flow(resolved_model_with_obs, obs)
    phase = HistoricalTripSamplingPhase(use_durations=True)
    result = phase.execute(_empty_state(), resolved, _period_row())
    arrivals = result.state.in_transit["arrival_period"].to_numpy()
    np.testing.assert_array_equal(arrivals, [0])
