"""Tests for OverflowRedirectPhase (skeleton + capacity-detection part)."""

from __future__ import annotations

import dataclasses

import pandas as pd
import pytest

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


def _attach_distance_matrix(
    resolved: ResolvedModelData,
    matrix: dict[tuple[str, str], float],
) -> ResolvedModelData:
    """Return a copy of *resolved* with a synthetic distance_matrix attached."""
    rows = [
        {"source_id": s, "target_id": t, "distance": d, "duration": d}
        for (s, t), d in matrix.items()
    ]
    return dataclasses.replace(resolved, distance_matrix=pd.DataFrame(rows))


def test_overflow_redirected_to_nearest_with_capacity(
    resolved_model: ResolvedModelData,
) -> None:
    """A single overflow row redirects exactly to the nearest free station."""
    _inject_storage_capacity(
        resolved_model, {"s1": 5.0, "s2": 10.0, "d1": 100.0},
    )
    resolved = _attach_distance_matrix(
        resolved_model,
        {
            ("s1", "s2"): 1.0, ("s2", "s1"): 1.0,
            ("s1", "d1"): 10.0, ("d1", "s1"): 10.0,
            ("s2", "d1"): 12.0, ("d1", "s2"): 12.0,
        },
    )
    inventory = pd.DataFrame(
        {
            "facility_id": ["s1", "s2", "d1"],
            "commodity_category": ["working_bike"] * 3,
            "quantity": [8.0, 3.0, 0.0],  # s1 over capacity by 3
        },
        columns=INVENTORY_COLUMNS,
    )
    state = _state_with_inventory(inventory)
    phase = OverflowRedirectPhase()
    result = phase.execute(state, resolved, _period_row())  # type: ignore[arg-type]

    redirect_df = result.events["redirected_flow"]
    assert len(redirect_df) == 1
    row = redirect_df.iloc[0]
    assert row["source_id"] == "s1"
    assert row["original_target_id"] == "s1"
    assert row["redirected_target_id"] == "s2"
    assert row["commodity_category"] == "working_bike"
    assert row["quantity"] == 3.0

    new_inv = result.state.inventory.set_index(
        ["facility_id", "commodity_category"]
    )["quantity"]
    assert new_inv[("s1", "working_bike")] == 5.0
    assert new_inv[("s2", "working_bike")] == 6.0


def test_redirect_per_commodity_isolation(
    resolved_model: ResolvedModelData,
) -> None:
    """A working_bike overflow must NOT consume electric_bike capacity."""
    cap_rows = pd.DataFrame({
        "facility_id": ["s1", "s2", "d1", "s2"],
        "operation_type": ["storage"] * 4,
        "commodity_category": [
            "working_bike", "working_bike", "working_bike", "electric_bike",
        ],
        "capacity": [5.0, 10.0, 100.0, 5.0],
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
    resolved_model.attributes.register_raw(spec, cap_rows)
    resolved = _attach_distance_matrix(
        resolved_model,
        {
            ("s1", "s2"): 1.0, ("s1", "d1"): 10.0,
            ("s2", "s1"): 1.0, ("s2", "d1"): 9.0,
            ("d1", "s1"): 10.0, ("d1", "s2"): 9.0,
        },
    )
    # s2 is FULL on working_bike (10/10) — its electric_bike capacity is
    # irrelevant to a working_bike overflow.  d1 is unbounded for working_bike.
    inventory = pd.DataFrame(
        {
            "facility_id": ["s1", "s2", "d1"],
            "commodity_category": ["working_bike", "working_bike", "working_bike"],
            "quantity": [8.0, 10.0, 0.0],
        },
        columns=INVENTORY_COLUMNS,
    )
    state = _state_with_inventory(inventory)
    phase = OverflowRedirectPhase()
    result = phase.execute(state, resolved, _period_row())  # type: ignore[arg-type]

    redirect_df = result.events["redirected_flow"]
    assert len(redirect_df) == 1
    # Must skip s2 (full on working_bike) and pick d1 despite longer distance.
    assert redirect_df.iloc[0]["redirected_target_id"] == "d1"


def test_argmin_lexicographic_tiebreak(
    resolved_model: ResolvedModelData,
) -> None:
    """When two candidates are equidistant, the lexicographically smaller wins.

    Encodes ADR Sec. 7.6: facility_ids are pre-sorted before argmin so np.argmin's
    "first minimum" rule gives a deterministic lexicographic tie-break.
    """
    _inject_storage_capacity(
        resolved_model, {"s1": 5.0, "s2": 10.0, "d1": 100.0},
    )
    resolved = _attach_distance_matrix(
        resolved_model,
        {
            ("s1", "s2"): 5.0, ("s1", "d1"): 5.0,
            ("s2", "s1"): 5.0, ("d1", "s1"): 5.0,
            ("s2", "d1"): 5.0, ("d1", "s2"): 5.0,
        },
    )
    inventory = pd.DataFrame(
        {
            "facility_id": ["s1", "s2", "d1"],
            "commodity_category": ["working_bike"] * 3,
            "quantity": [8.0, 0.0, 0.0],
        },
        columns=INVENTORY_COLUMNS,
    )
    state = _state_with_inventory(inventory)
    phase = OverflowRedirectPhase()
    result = phase.execute(state, resolved, _period_row())  # type: ignore[arg-type]

    redirect_df = result.events["redirected_flow"]
    # Sorted facility list: ['d1', 's1', 's2']; first equidistant winner = 'd1'.
    assert redirect_df.iloc[0]["redirected_target_id"] == "d1"


def test_redirect_proportional_when_winner_oversubscribed(
    resolved_model: ResolvedModelData,
) -> None:
    """Two overflowing sources sharing a single winner split capacity proportionally."""
    _inject_storage_capacity(
        resolved_model, {"s1": 5.0, "s2": 5.0, "d1": 5.0},
    )
    resolved = _attach_distance_matrix(
        resolved_model,
        {
            ("s1", "s2"): 1.0, ("s1", "d1"): 10.0,
            ("s2", "s1"): 1.0, ("s2", "d1"): 10.0,
            ("d1", "s1"): 10.0, ("d1", "s2"): 10.0,
        },
    )
    # Both s1 and s2 overflow; only d1 has free space (and it's distant).
    # But s1 and s2 are also full so they won't accept each other's overflow.
    inventory = pd.DataFrame(
        {
            "facility_id": ["s1", "s2", "d1"],
            "commodity_category": ["working_bike"] * 3,
            "quantity": [9.0, 9.0, 0.0],  # overflow s1=4, s2=4; d1 free=5
        },
        columns=INVENTORY_COLUMNS,
    )
    state = _state_with_inventory(inventory)
    phase = OverflowRedirectPhase()
    result = phase.execute(state, resolved, _period_row())  # type: ignore[arg-type]

    redirect_df = result.events["redirected_flow"]
    # Both rows go to d1; total accepted clamped to d1.capacity (5).
    assert len(redirect_df) == 2
    assert (redirect_df["redirected_target_id"] == "d1").all()
    assert redirect_df["quantity"].sum() == pytest.approx(5.0)
    # Each source contributes proportionally: 4 / (4+4) * 5 = 2.5
    assert redirect_df.iloc[0]["quantity"] == pytest.approx(2.5)
    assert redirect_df.iloc[1]["quantity"] == pytest.approx(2.5)
