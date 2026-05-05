"""Tests for OverflowRedirectPhase (skeleton + capacity-detection part)."""

from __future__ import annotations

import dataclasses

import pandas as pd

from gbp.consumers.simulator.built_in_phases import OverflowRedirectPhase
from gbp.consumers.simulator.state import (
    IN_TRANSIT_COLUMNS,
    INVENTORY_COLUMNS,
    SimulationState,
)
from gbp.core.attributes.spec import AttributeSpec
from gbp.core.enums import AttributeKind
from gbp.core.model import ResolvedModelData


def _state_with_inventory(inventory: pd.DataFrame) -> SimulationState:
    """Construct a minimal :class:`SimulationState` for redirect tests."""
    return SimulationState(
        period_index=0,
        period_id="p0",
        inventory=inventory,
        in_transit=pd.DataFrame(columns=IN_TRANSIT_COLUMNS),
        resources=pd.DataFrame(columns=["resource_id", "resource_category",
                                        "home_facility_id", "current_facility_id",
                                        "status", "available_at_period"]),
    )


def _inject_storage_capacity(
    resolved: ResolvedModelData,
    capacities: dict[str, float],
    commodity: str = "working_bike",
) -> None:
    """Inject an ``operation_capacity[storage]`` attribute for *resolved*."""
    cap_data = pd.DataFrame({
        "facility_id": list(capacities.keys()),
        "operation_type": ["storage"] * len(capacities),
        "commodity_category": [commodity] * len(capacities),
        "capacity": list(capacities.values()),
    })
    spec = AttributeSpec(
        name="operation_capacity",
        kind=AttributeKind.CAPACITY,
        entity_type="facility",
        grain=("facility_id", "operation_type", "commodity_category"),
        resolved_grain=("facility_id", "operation_type", "commodity_category"),
        value_column="capacity",
        source_table="operation_capacity",
        unit=None,
        aggregation="min",
        nullable=True,
        eav_filter=None,
    )
    resolved.attributes.register_raw(spec, cap_data)


def _period_row() -> object:
    """Return a duck-typed period row carrying only what the phase reads."""
    return dataclasses.make_dataclass(
        "PeriodRow", [("period_index", int), ("period_id", str)]
    )(0, "p0")


def test_no_overflow_is_noop(resolved_model: ResolvedModelData) -> None:
    """Capacity strictly above inventory → no events emitted."""
    _inject_storage_capacity(resolved_model, {"s1": 100.0, "s2": 100.0})
    inventory = pd.DataFrame(
        {
            "facility_id": ["s1", "s2"],
            "commodity_category": ["working_bike", "working_bike"],
            "quantity": [10.0, 5.0],
        },
        columns=INVENTORY_COLUMNS,
    )
    state = _state_with_inventory(inventory)
    phase = OverflowRedirectPhase()
    result = phase.execute(state, resolved_model, _period_row())  # type: ignore[arg-type]
    assert result.events == {}
    pd.testing.assert_frame_equal(
        result.state.inventory.reset_index(drop=True),
        inventory.reset_index(drop=True),
    )


def test_no_capacity_attribute_is_noop(
    resolved_model: ResolvedModelData,
) -> None:
    """Without an operation_capacity attribute, phase is a no-op."""
    inventory = pd.DataFrame(
        {
            "facility_id": ["s1"],
            "commodity_category": ["working_bike"],
            "quantity": [50.0],
        },
        columns=INVENTORY_COLUMNS,
    )
    state = _state_with_inventory(inventory)
    phase = OverflowRedirectPhase()
    result = phase.execute(state, resolved_model, _period_row())  # type: ignore[arg-type]
    assert result.events == {}


def test_facility_without_storage_unbounded(
    resolved_model: ResolvedModelData,
) -> None:
    """Facilities lacking a storage capacity row are treated as unbounded.

    Mirrors :class:`DockCapacityPhase` semantics — when a facility has no
    ``operation_capacity[storage]`` row, no overflow is recorded for it
    even when its inventory is arbitrarily large.
    """
    _inject_storage_capacity(resolved_model, {"s1": 5.0})  # only s1 bounded
    inventory = pd.DataFrame(
        {
            "facility_id": ["s1", "s2"],
            "commodity_category": ["working_bike", "working_bike"],
            "quantity": [3.0, 999.0],  # s2 way over hypothetical capacity
        },
        columns=INVENTORY_COLUMNS,
    )
    state = _state_with_inventory(inventory)
    phase = OverflowRedirectPhase()
    result = phase.execute(state, resolved_model, _period_row())  # type: ignore[arg-type]
    # No overflow recorded because s1 is under capacity and s2 is unbounded.
    assert result.events == {}


def test_overflow_detected_skeleton_returns_empty(
    resolved_model: ResolvedModelData,
) -> None:
    """When overflow is present but search is TODO, phase returns empty.

    The skeleton intentionally does not yet implement the nearest-with-capacity
    arg-min (see TODO inside :class:`OverflowRedirectPhase.execute`).  The
    phase therefore emits no redirect events even when overflow exists.
    Once the TODO is resolved, this test will be replaced by assertions
    on the emitted ``redirected_flow`` rows.
    """
    _inject_storage_capacity(resolved_model, {"s1": 1.0, "s2": 50.0})
    inventory = pd.DataFrame(
        {
            "facility_id": ["s1", "s2"],
            "commodity_category": ["working_bike", "working_bike"],
            "quantity": [10.0, 5.0],  # s1 over by 9
        },
        columns=INVENTORY_COLUMNS,
    )
    state = _state_with_inventory(inventory)
    phase = OverflowRedirectPhase()
    result = phase.execute(state, resolved_model, _period_row())  # type: ignore[arg-type]
    assert result.events == {}
