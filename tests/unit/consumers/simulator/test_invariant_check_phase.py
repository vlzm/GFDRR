"""Tests for InvariantCheckPhase."""

from __future__ import annotations

import dataclasses

import pandas as pd
import pytest

from gbp.consumers.simulator.built_in_phases import (
    InvariantCheckPhase,
    InvariantViolationError,
)
from gbp.consumers.simulator.state import (
    IN_TRANSIT_COLUMNS,
    INVENTORY_COLUMNS,
    SimulationState,
)
from gbp.core.model import ResolvedModelData


def _state_with_inventory(
    inventory: pd.DataFrame,
    in_transit: pd.DataFrame | None = None,
) -> SimulationState:
    """Build a minimal :class:`SimulationState` for invariant tests."""
    return SimulationState(
        period_index=0,
        period_id="p0",
        inventory=inventory,
        in_transit=(
            in_transit
            if in_transit is not None
            else pd.DataFrame(columns=IN_TRANSIT_COLUMNS)
        ),
        resources=pd.DataFrame(columns=["resource_id", "resource_category",
                                        "home_facility_id", "current_facility_id",
                                        "status", "available_at_period"]),
    )


def _period_row() -> object:
    """Return a duck-typed period row carrying only the fields used here."""
    return dataclasses.make_dataclass(
        "PeriodRow", [("period_index", int), ("period_id", str)]
    )(0, "p0")


def test_first_execute_captures_baseline_no_log(
    resolved_model: ResolvedModelData,
) -> None:
    """First execute with baseline=None captures totals into intermediates."""
    inventory = pd.DataFrame(
        {
            "facility_id": ["s1", "s2"],
            "commodity_category": ["working_bike", "working_bike"],
            "quantity": [10.0, 5.0],
        },
        columns=INVENTORY_COLUMNS,
    )
    state = _state_with_inventory(inventory)
    phase = InvariantCheckPhase()
    result = phase.execute(state, resolved_model, _period_row())  # type: ignore[arg-type]
    assert result.events == {}
    assert result.state.intermediates["invariant_baseline"] == {"working_bike": 15.0}


def test_invariant_holds_when_inventory_stable(
    resolved_model: ResolvedModelData,
) -> None:
    """When the per-commodity total matches the baseline, no violation."""
    inventory = pd.DataFrame(
        {
            "facility_id": ["s1", "s2"],
            "commodity_category": ["working_bike", "working_bike"],
            "quantity": [10.0, 5.0],
        },
        columns=INVENTORY_COLUMNS,
    )
    state = _state_with_inventory(inventory)
    phase = InvariantCheckPhase(baseline={"working_bike": 15.0})
    result = phase.execute(state, resolved_model, _period_row())  # type: ignore[arg-type]
    assert result.events == {}


def test_invariant_violation_raises_by_default(
    resolved_model: ResolvedModelData,
) -> None:
    """fail_on_violation=True (default) raises on per-commodity drift."""
    inventory = pd.DataFrame(
        {
            "facility_id": ["s1"],
            "commodity_category": ["working_bike"],
            "quantity": [3.0],
        },
        columns=INVENTORY_COLUMNS,
    )
    state = _state_with_inventory(inventory)
    phase = InvariantCheckPhase(baseline={"working_bike": 15.0})
    with pytest.raises(InvariantViolationError):
        phase.execute(state, resolved_model, _period_row())  # type: ignore[arg-type]


def test_invariant_violation_logged_when_fail_off(
    resolved_model: ResolvedModelData,
) -> None:
    """fail_on_violation=False emits a row instead of raising."""
    inventory = pd.DataFrame(
        {
            "facility_id": ["s1"],
            "commodity_category": ["working_bike"],
            "quantity": [3.0],
        },
        columns=INVENTORY_COLUMNS,
    )
    state = _state_with_inventory(inventory)
    phase = InvariantCheckPhase(
        baseline={"working_bike": 15.0}, fail_on_violation=False,
    )
    result = phase.execute(state, resolved_model, _period_row())  # type: ignore[arg-type]
    assert "invariant_violation" in result.events
    df = result.events["invariant_violation"]
    assert list(df["commodity_category"]) == ["working_bike"]
    assert df["baseline"].iloc[0] == 15.0
    assert df["current"].iloc[0] == 3.0
    assert df["delta"].iloc[0] == -12.0


def test_invariant_per_commodity_detects_swap(
    resolved_model: ResolvedModelData,
) -> None:
    """Per-commodity invariant catches a A→B swap that aggregate sum hides."""
    inventory = pd.DataFrame(
        {
            "facility_id": ["s1", "s2"],
            "commodity_category": ["working_bike", "electric_bike"],
            "quantity": [0.0, 15.0],
        },
        columns=INVENTORY_COLUMNS,
    )
    state = _state_with_inventory(inventory)
    phase = InvariantCheckPhase(
        baseline={"working_bike": 15.0, "electric_bike": 0.0},
    )
    with pytest.raises(InvariantViolationError):
        phase.execute(state, resolved_model, _period_row())  # type: ignore[arg-type]


def test_invariant_check_skips_inventory_absent_commodity(
    resolved_model: ResolvedModelData,
) -> None:
    """Commodities absent from the baseline are excluded — no spurious raise.

    A flow-only commodity that appears in observed_flow but never in
    inventory_initial is excluded from the baseline.  When inventory grows
    to include that commodity (e.g. an arrival), the invariant phase MUST
    NOT flag it because the baseline does not track it.  ADR Sec. 7.7.
    """
    inventory = pd.DataFrame(
        {
            "facility_id": ["s1", "s2"],
            "commodity_category": ["working_bike", "cargo_bike"],
            "quantity": [15.0, 4.0],
        },
        columns=INVENTORY_COLUMNS,
    )
    state = _state_with_inventory(inventory)
    phase = InvariantCheckPhase(baseline={"working_bike": 15.0})
    result = phase.execute(state, resolved_model, _period_row())  # type: ignore[arg-type]
    assert result.events == {}


def test_invariant_includes_in_transit(
    resolved_model: ResolvedModelData,
) -> None:
    """Bikes counted in in_transit do count toward the invariant total."""
    inventory = pd.DataFrame(
        {
            "facility_id": ["s1"],
            "commodity_category": ["working_bike"],
            "quantity": [10.0],
        },
        columns=INVENTORY_COLUMNS,
    )
    in_transit = pd.DataFrame(
        {
            "shipment_id": ["t0"],
            "source_id": ["s1"],
            "target_id": ["s2"],
            "commodity_category": ["working_bike"],
            "quantity": [5.0],
            "resource_id": [None],
            "departure_period": [0],
            "arrival_period": [1],
        },
        columns=IN_TRANSIT_COLUMNS,
    )
    state = _state_with_inventory(inventory, in_transit)
    phase = InvariantCheckPhase(baseline={"working_bike": 15.0})
    result = phase.execute(state, resolved_model, _period_row())  # type: ignore[arg-type]
    assert result.events == {}
