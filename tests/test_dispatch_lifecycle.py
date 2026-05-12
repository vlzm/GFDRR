"""Unit tests for the dispatch lifecycle.

Tests ``run_dispatch_lifecycle`` with hand-crafted DataFrames covering each
rejection reason, successful dispatch application, and auto-assignment of
resources.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from gbp.consumers.simulator.dispatch_lifecycle import run_dispatch_lifecycle
from gbp.consumers.simulator.log import RejectReason
from gbp.consumers.simulator.state import (
    IN_TRANSIT_COLUMNS,
    INVENTORY_COLUMNS,
    RESOURCE_COLUMNS,
    PeriodRow,
    SimulationState,
)
from gbp.core.enums import ModalType, ResourceStatus
from gbp.core.model import ResolvedModelData

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PERIOD = PeriodRow(
    Index=0,
    period_id="p_0",
    planning_horizon_id="h0",
    segment_index=0,
    period_index=0,
    period_type="hour",
    start_date=date(2025, 1, 1),
    end_date=date(2025, 1, 1),
)


def _make_inventory(rows: list[tuple[str, str, float]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=INVENTORY_COLUMNS)


def _make_resources(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=RESOURCE_COLUMNS)
    return pd.DataFrame(rows)[RESOURCE_COLUMNS]


def _make_dispatches(rows: list[dict]) -> pd.DataFrame:
    cols = [
        "source_id", "target_id", "commodity_category",
        "quantity", "resource_id", "modal_type", "arrival_period",
    ]
    return pd.DataFrame(rows, columns=cols)


def _make_state(
    inventory_rows: list[tuple[str, str, float]],
    resource_rows: list[dict] | None = None,
) -> SimulationState:
    return SimulationState(
        period_index=0,
        period_id="p_0",
        inventory=_make_inventory(inventory_rows),
        in_transit=pd.DataFrame(columns=IN_TRANSIT_COLUMNS),
        resources=_make_resources(resource_rows or []),
    )


def _make_resolved(
    edges: pd.DataFrame | None = None,
    resource_categories: pd.DataFrame | None = None,
    resource_commodity_compat: pd.DataFrame | None = None,
    resource_modal_compat: pd.DataFrame | None = None,
) -> ResolvedModelData:
    """Build a minimal ResolvedModelData stub for lifecycle tests.

    Only the fields used by the lifecycle are populated; every other field
    is set to an empty DataFrame (the default for ResolvedModelData).
    """
    import dataclasses

    # Start from a minimal ResolvedModelData with all-empty tables.
    # We need periods for the build, but lifecycle doesn't use them.
    from gbp.core.model import ResolvedModelData as RMD

    fields = {f.name: pd.DataFrame() for f in dataclasses.fields(RMD)}
    fields["periods"] = pd.DataFrame(
        {"period_id": ["p_0"], "period_index": [0]}
    )
    if edges is not None:
        fields["edges"] = edges
    if resource_categories is not None:
        fields["resource_categories"] = resource_categories
    if resource_commodity_compat is not None:
        fields["resource_commodity_compatibility"] = resource_commodity_compat
    if resource_modal_compat is not None:
        fields["resource_modal_compatibility"] = resource_modal_compat
    return RMD(**fields)


# ---------------------------------------------------------------------------
# Test: empty dispatches
# ---------------------------------------------------------------------------


class TestEmptyDispatches:
    def test_empty_input_returns_unchanged_state(self):
        state = _make_state([("A", "bike", 10.0)])
        resolved = _make_resolved()
        dispatches = _make_dispatches([])

        outcome = run_dispatch_lifecycle(dispatches, state, resolved, _PERIOD)

        assert outcome.rejected.empty
        assert outcome.flow_events.empty
        assert outcome.state is state


# ---------------------------------------------------------------------------
# Test: successful dispatch
# ---------------------------------------------------------------------------


class TestSuccessfulDispatch:
    def test_dispatch_decrements_inventory_and_creates_shipment(self):
        state = _make_state(
            inventory_rows=[("A", "bike", 10.0), ("B", "bike", 5.0)],
            resource_rows=[{
                "resource_id": "truck_1",
                "resource_category": "truck",
                "home_facility_id": "A",
                "current_facility_id": "A",
                "status": ResourceStatus.AVAILABLE.value,
                "available_at_period": None,
            }],
        )
        resolved = _make_resolved(
            resource_categories=pd.DataFrame({
                "resource_category_id": ["truck"],
                "base_capacity": [100.0],
            }),
        )
        dispatches = _make_dispatches([{
            "source_id": "A",
            "target_id": "B",
            "commodity_category": "bike",
            "quantity": 3.0,
            "resource_id": "truck_1",
            "modal_type": ModalType.ROAD.value,
            "arrival_period": 1,
        }])

        outcome = run_dispatch_lifecycle(dispatches, state, resolved, _PERIOD)

        assert outcome.rejected.empty
        assert len(outcome.flow_events) == 1

        # Source inventory decremented
        new_inv = outcome.state.inventory
        a_qty = new_inv.loc[new_inv["facility_id"] == "A", "quantity"].iloc[0]
        assert a_qty == pytest.approx(7.0)

        # Shipment in transit
        assert len(outcome.state.in_transit) == 1
        shp = outcome.state.in_transit.iloc[0]
        assert shp["source_id"] == "A"
        assert shp["target_id"] == "B"
        assert shp["quantity"] == 3.0

        # Resource marked as IN_TRANSIT
        res = outcome.state.resources
        truck = res.loc[res["resource_id"] == "truck_1"].iloc[0]
        assert truck["status"] == ResourceStatus.IN_TRANSIT.value


# ---------------------------------------------------------------------------
# Test: rejection reasons
# ---------------------------------------------------------------------------


class TestRejectInvalidArrival:
    def test_arrival_before_current_period_is_rejected(self):
        period = PeriodRow(
            Index=5, period_id="p_5", planning_horizon_id="h0",
            segment_index=0, period_index=5, period_type="hour",
            start_date=date(2025, 1, 1), end_date=date(2025, 1, 1),
        )
        state = _make_state(
            inventory_rows=[("A", "bike", 10.0)],
            resource_rows=[{
                "resource_id": "truck_1",
                "resource_category": "truck",
                "home_facility_id": "A",
                "current_facility_id": "A",
                "status": ResourceStatus.AVAILABLE.value,
                "available_at_period": None,
            }],
        )
        resolved = _make_resolved()
        dispatches = _make_dispatches([{
            "source_id": "A",
            "target_id": "B",
            "commodity_category": "bike",
            "quantity": 1.0,
            "resource_id": "truck_1",
            "modal_type": ModalType.ROAD.value,
            "arrival_period": 3,  # before period_index=5
        }])

        outcome = run_dispatch_lifecycle(dispatches, state, resolved, period)

        assert len(outcome.rejected) == 1
        assert outcome.rejected.iloc[0]["reason"] == RejectReason.INVALID_ARRIVAL.value
        assert outcome.flow_events.empty


class TestRejectInvalidEdge:
    def test_dispatch_on_nonexistent_edge_is_rejected(self):
        state = _make_state(
            inventory_rows=[("A", "bike", 10.0)],
            resource_rows=[{
                "resource_id": "truck_1",
                "resource_category": "truck",
                "home_facility_id": "A",
                "current_facility_id": "A",
                "status": ResourceStatus.AVAILABLE.value,
                "available_at_period": None,
            }],
        )
        edges = pd.DataFrame({
            "source_id": ["A"],
            "target_id": ["C"],  # only A->C exists
            "modal_type": [ModalType.ROAD.value],
        })
        resolved = _make_resolved(edges=edges)
        dispatches = _make_dispatches([{
            "source_id": "A",
            "target_id": "B",  # A->B does not exist
            "commodity_category": "bike",
            "quantity": 1.0,
            "resource_id": "truck_1",
            "modal_type": ModalType.ROAD.value,
            "arrival_period": 1,
        }])

        outcome = run_dispatch_lifecycle(dispatches, state, resolved, _PERIOD)

        assert len(outcome.rejected) == 1
        assert outcome.rejected.iloc[0]["reason"] == RejectReason.INVALID_EDGE.value


class TestRejectUnavailableResource:
    def test_dispatch_with_unavailable_resource_is_rejected(self):
        state = _make_state(
            inventory_rows=[("A", "bike", 10.0)],
            resource_rows=[{
                "resource_id": "truck_1",
                "resource_category": "truck",
                "home_facility_id": "A",
                "current_facility_id": "A",
                "status": ResourceStatus.IN_TRANSIT.value,  # not available
                "available_at_period": 5,
            }],
        )
        resolved = _make_resolved()
        dispatches = _make_dispatches([{
            "source_id": "A",
            "target_id": "B",
            "commodity_category": "bike",
            "quantity": 1.0,
            "resource_id": "truck_1",
            "modal_type": ModalType.ROAD.value,
            "arrival_period": 1,
        }])

        outcome = run_dispatch_lifecycle(dispatches, state, resolved, _PERIOD)

        assert len(outcome.rejected) == 1
        assert outcome.rejected.iloc[0]["reason"] == RejectReason.NO_AVAILABLE_RESOURCE.value


class TestRejectOverCapacity:
    def test_dispatch_exceeding_resource_capacity_is_rejected(self):
        state = _make_state(
            inventory_rows=[("A", "bike", 100.0)],
            resource_rows=[{
                "resource_id": "truck_1",
                "resource_category": "truck",
                "home_facility_id": "A",
                "current_facility_id": "A",
                "status": ResourceStatus.AVAILABLE.value,
                "available_at_period": None,
            }],
        )
        resolved = _make_resolved(
            resource_categories=pd.DataFrame({
                "resource_category_id": ["truck"],
                "base_capacity": [5.0],  # capacity = 5
            }),
        )
        dispatches = _make_dispatches([{
            "source_id": "A",
            "target_id": "B",
            "commodity_category": "bike",
            "quantity": 10.0,  # exceeds capacity
            "resource_id": "truck_1",
            "modal_type": ModalType.ROAD.value,
            "arrival_period": 1,
        }])

        outcome = run_dispatch_lifecycle(dispatches, state, resolved, _PERIOD)

        assert len(outcome.rejected) == 1
        assert outcome.rejected.iloc[0]["reason"] == RejectReason.OVER_CAPACITY.value


class TestRejectInsufficientInventory:
    def test_dispatch_exceeding_stock_is_rejected(self):
        state = _make_state(
            inventory_rows=[("A", "bike", 2.0)],
            resource_rows=[{
                "resource_id": "truck_1",
                "resource_category": "truck",
                "home_facility_id": "A",
                "current_facility_id": "A",
                "status": ResourceStatus.AVAILABLE.value,
                "available_at_period": None,
            }],
        )
        resolved = _make_resolved(
            resource_categories=pd.DataFrame({
                "resource_category_id": ["truck"],
                "base_capacity": [100.0],
            }),
        )
        dispatches = _make_dispatches([{
            "source_id": "A",
            "target_id": "B",
            "commodity_category": "bike",
            "quantity": 5.0,  # only 2 in stock
            "resource_id": "truck_1",
            "modal_type": ModalType.ROAD.value,
            "arrival_period": 1,
        }])

        outcome = run_dispatch_lifecycle(dispatches, state, resolved, _PERIOD)

        assert len(outcome.rejected) == 1
        assert outcome.rejected.iloc[0]["reason"] == RejectReason.INSUFFICIENT_INVENTORY.value


class TestRejectFirstRuleWins:
    """First-match semantics: earlier rules shadow later ones."""

    def test_invalid_arrival_shadows_insufficient_inventory(self):
        """Dispatch with bad arrival AND insufficient stock gets INVALID_ARRIVAL."""
        period = PeriodRow(
            Index=5, period_id="p_5", planning_horizon_id="h0",
            segment_index=0, period_index=5, period_type="hour",
            start_date=date(2025, 1, 1), end_date=date(2025, 1, 1),
        )
        state = _make_state(
            inventory_rows=[("A", "bike", 0.0)],  # no stock
            resource_rows=[{
                "resource_id": "truck_1",
                "resource_category": "truck",
                "home_facility_id": "A",
                "current_facility_id": "A",
                "status": ResourceStatus.AVAILABLE.value,
                "available_at_period": None,
            }],
        )
        resolved = _make_resolved(
            resource_categories=pd.DataFrame({
                "resource_category_id": ["truck"],
                "base_capacity": [100.0],
            }),
        )
        dispatches = _make_dispatches([{
            "source_id": "A",
            "target_id": "B",
            "commodity_category": "bike",
            "quantity": 5.0,
            "resource_id": "truck_1",
            "modal_type": ModalType.ROAD.value,
            "arrival_period": 2,  # before period_index=5
        }])

        outcome = run_dispatch_lifecycle(dispatches, state, resolved, period)

        assert len(outcome.rejected) == 1
        assert outcome.rejected.iloc[0]["reason"] == RejectReason.INVALID_ARRIVAL.value


# ---------------------------------------------------------------------------
# Test: auto-assignment of resources
# ---------------------------------------------------------------------------


class TestResourceAutoAssignment:
    def test_null_resource_is_auto_assigned(self):
        state = _make_state(
            inventory_rows=[("A", "bike", 10.0)],
            resource_rows=[{
                "resource_id": "truck_1",
                "resource_category": "truck",
                "home_facility_id": "A",
                "current_facility_id": "A",
                "status": ResourceStatus.AVAILABLE.value,
                "available_at_period": None,
            }],
        )
        resolved = _make_resolved(
            resource_categories=pd.DataFrame({
                "resource_category_id": ["truck"],
                "base_capacity": [100.0],
            }),
        )
        dispatches = _make_dispatches([{
            "source_id": "A",
            "target_id": "B",
            "commodity_category": "bike",
            "quantity": 3.0,
            "resource_id": None,  # should be auto-assigned
            "modal_type": ModalType.ROAD.value,
            "arrival_period": 1,
        }])

        outcome = run_dispatch_lifecycle(dispatches, state, resolved, _PERIOD)

        assert outcome.rejected.empty
        assert len(outcome.flow_events) == 1
        # truck_1 was assigned
        assert outcome.flow_events.iloc[0]["resource_id"] == "truck_1"

    def test_two_dispatches_get_different_resources(self):
        state = _make_state(
            inventory_rows=[("A", "bike", 20.0)],
            resource_rows=[
                {
                    "resource_id": "truck_1",
                    "resource_category": "truck",
                    "home_facility_id": "A",
                    "current_facility_id": "A",
                    "status": ResourceStatus.AVAILABLE.value,
                    "available_at_period": None,
                },
                {
                    "resource_id": "truck_2",
                    "resource_category": "truck",
                    "home_facility_id": "A",
                    "current_facility_id": "A",
                    "status": ResourceStatus.AVAILABLE.value,
                    "available_at_period": None,
                },
            ],
        )
        resolved = _make_resolved(
            resource_categories=pd.DataFrame({
                "resource_category_id": ["truck"],
                "base_capacity": [100.0],
            }),
        )
        dispatches = _make_dispatches([
            {
                "source_id": "A", "target_id": "B", "commodity_category": "bike",
                "quantity": 3.0, "resource_id": None, "modal_type": ModalType.ROAD.value,
                "arrival_period": 1,
            },
            {
                "source_id": "A", "target_id": "C", "commodity_category": "bike",
                "quantity": 4.0, "resource_id": None, "modal_type": ModalType.ROAD.value,
                "arrival_period": 1,
            },
        ])

        outcome = run_dispatch_lifecycle(dispatches, state, resolved, _PERIOD)

        assert outcome.rejected.empty
        assert len(outcome.flow_events) == 2
        assigned_resources = set(outcome.flow_events["resource_id"])
        assert assigned_resources == {"truck_1", "truck_2"}


# ---------------------------------------------------------------------------
# Test: sequential inventory consumption
# ---------------------------------------------------------------------------


class TestSequentialInventoryConsumption:
    def test_later_dispatch_rejected_when_earlier_consumes_stock(self):
        """Two dispatches from A, but only enough stock for one."""
        state = _make_state(
            inventory_rows=[("A", "bike", 5.0)],
            resource_rows=[
                {
                    "resource_id": "truck_1",
                    "resource_category": "truck",
                    "home_facility_id": "A",
                    "current_facility_id": "A",
                    "status": ResourceStatus.AVAILABLE.value,
                    "available_at_period": None,
                },
                {
                    "resource_id": "truck_2",
                    "resource_category": "truck",
                    "home_facility_id": "A",
                    "current_facility_id": "A",
                    "status": ResourceStatus.AVAILABLE.value,
                    "available_at_period": None,
                },
            ],
        )
        resolved = _make_resolved(
            resource_categories=pd.DataFrame({
                "resource_category_id": ["truck"],
                "base_capacity": [100.0],
            }),
        )
        dispatches = _make_dispatches([
            {
                "source_id": "A", "target_id": "B", "commodity_category": "bike",
                "quantity": 3.0, "resource_id": "truck_1",
                "modal_type": ModalType.ROAD.value, "arrival_period": 1,
            },
            {
                "source_id": "A", "target_id": "C", "commodity_category": "bike",
                "quantity": 4.0, "resource_id": "truck_2",
                "modal_type": ModalType.ROAD.value, "arrival_period": 1,
            },
        ])

        outcome = run_dispatch_lifecycle(dispatches, state, resolved, _PERIOD)

        # First dispatch succeeds (3 <= 5), second is rejected (3+4=7 > 5)
        assert len(outcome.flow_events) == 1
        assert len(outcome.rejected) == 1
        assert outcome.rejected.iloc[0]["reason"] == RejectReason.INSUFFICIENT_INVENTORY.value
        assert outcome.flow_events.iloc[0]["quantity"] == 3.0
