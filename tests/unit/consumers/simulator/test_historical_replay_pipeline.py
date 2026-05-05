"""Smoke test: the canonical historical-replay pipeline composes and runs.

Validates the form-factor contract from the deep-interview spec: the user
constructs a list of phases, hands it to ``EnvironmentConfig``, and calls
``env.run()``.  The pipeline must run to completion on the reference
fixture and the skeleton ``OverflowRedirectPhase`` must emit no events
because ``observed_flow.target_id`` is post-redirect by spec Constraint 3.
"""

from __future__ import annotations

import dataclasses

import pandas as pd

from gbp.consumers.simulator.built_in_phases import (
    ArrivalsPhase,
    DeparturePhysicsPhase,
    HistoricalLatentDemandPhase,
    HistoricalODStructurePhase,
    HistoricalTripSamplingPhase,
    InvariantCheckPhase,
    LatentDemandInflatorPhase,
    OverflowRedirectPhase,
)
from gbp.consumers.simulator.config import EnvironmentConfig
from gbp.consumers.simulator.engine import Environment
from gbp.consumers.simulator.state import SimulationState, init_state
from gbp.core.model import ResolvedModelData


def _baseline_per_commodity(state: SimulationState) -> dict[str, float]:
    """Compute per-commodity baseline ``Σ inventory + Σ in_transit``."""
    if state.inventory.empty:
        inv_totals = pd.Series(dtype=float)
    else:
        inv_totals = (
            state.inventory.groupby("commodity_category")["quantity"].sum()
        )
    if state.in_transit.empty:
        transit_totals = pd.Series(dtype=float)
    else:
        transit_totals = (
            state.in_transit.groupby("commodity_category")["quantity"].sum()
        )
    combined = inv_totals.add(transit_totals, fill_value=0.0)
    return {str(k): float(v) for k, v in combined.items()}


def test_canonical_replay_pipeline_runs_clean(
    resolved_model_with_obs: ResolvedModelData,
) -> None:
    """Canonical historical-replay pipeline composes and runs to completion."""
    resolved = dataclasses.replace(resolved_model_with_obs, supply=None)
    state = init_state(resolved)
    baseline = _baseline_per_commodity(state)

    phases = [
        HistoricalLatentDemandPhase(),
        HistoricalODStructurePhase(),
        DeparturePhysicsPhase(mode="permissive"),
        HistoricalTripSamplingPhase(use_durations=True),
        ArrivalsPhase(),
        OverflowRedirectPhase(),
        InvariantCheckPhase(baseline=baseline, fail_on_violation=True),
    ]
    env = Environment(
        resolved,
        EnvironmentConfig(
            phases=phases, seed=42, scenario_id="historical_replay",
        ),
    )
    log = env.run()

    tables = log.to_dataframes()
    # Skeleton OverflowRedirectPhase emits no events on canonical data
    # because target_id is post-redirect (spec Constraint 3).
    assert tables["simulation_redirected_flow_log"].empty
    # InvariantCheckPhase did not raise -> per-commodity invariant held.
    assert tables["simulation_invariant_violation_log"].empty


def test_canonical_replay_composes_with_inflator(
    resolved_model_with_obs: ResolvedModelData,
) -> None:
    """Pipeline still runs when LatentDemandInflatorPhase is inserted."""
    resolved = dataclasses.replace(resolved_model_with_obs, supply=None)
    state = init_state(resolved)
    baseline = _baseline_per_commodity(state)

    phases = [
        HistoricalLatentDemandPhase(),
        LatentDemandInflatorPhase(multiplier=1.0),  # identity at this multiplier
        HistoricalODStructurePhase(),
        DeparturePhysicsPhase(mode="permissive"),
        HistoricalTripSamplingPhase(use_durations=True),
        ArrivalsPhase(),
        OverflowRedirectPhase(),
        InvariantCheckPhase(baseline=baseline, fail_on_violation=True),
    ]
    env = Environment(
        resolved,
        EnvironmentConfig(phases=phases, seed=42),
    )
    env.run()  # must not raise
