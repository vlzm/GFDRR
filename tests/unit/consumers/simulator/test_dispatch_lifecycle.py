"""Tests for the dispatch lifecycle.

Tests target :func:`run_dispatch_lifecycle` directly, without going through a
``DispatchPhase`` or constructing a fake ``Task``.  The lifecycle is the deep
module — its interface is the test surface.
"""
# ruff: noqa: D102

from __future__ import annotations

from datetime import date

import pandas as pd

from gbp.consumers.simulator.dispatch_lifecycle import (
    DispatchOutcome,
    _reject_unavailable_resource,
    run_dispatch_lifecycle,
)
from gbp.consumers.simulator.log import RejectReason
from gbp.consumers.simulator.state import PeriodRow, init_state
from gbp.consumers.simulator.task import DISPATCH_COLUMNS
from gbp.core.enums import ResourceStatus
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


def _single_dispatch(
    *, period_index: int = 0, source_id: str = "d1", target_id: str = "s1",
    quantity: float = 5.0, resource_id: object = None,
) -> pd.DataFrame:
    """One dispatch row with the standard ``DISPATCH_COLUMNS`` shape."""
    return pd.DataFrame(
        {
            "source_id": [source_id],
            "target_id": [target_id],
            "commodity_category": ["working_bike"],
            "quantity": [quantity],
            "resource_id": [resource_id],
            "modal_type": ["road"],
            "arrival_period": [period_index + 1],
        }
    )


class TestEmptyDispatches:
    """Empty input short-circuits without touching state or building events."""

    def test_state_passes_through_unchanged(
        self, resolved_model: ResolvedModelData,
    ) -> None:
        state = init_state(resolved_model)
        outcome = run_dispatch_lifecycle(
            pd.DataFrame(columns=DISPATCH_COLUMNS),
            state, resolved_model, _make_period(),
        )
        assert isinstance(outcome, DispatchOutcome)
        assert outcome.state is state
        assert outcome.flow_events.empty
        assert outcome.rejected.empty


class TestApply:
    """A valid dispatch advances state and produces a flow event."""

    def test_inventory_decremented_at_source(
        self, resolved_model: ResolvedModelData,
    ) -> None:
        state = init_state(resolved_model)
        outcome = run_dispatch_lifecycle(
            _single_dispatch(), state, resolved_model, _make_period(),
        )
        d1_qty = outcome.state.inventory.loc[
            outcome.state.inventory["facility_id"] == "d1", "quantity"
        ].iloc[0]
        assert d1_qty == 50.0 - 5.0

    def test_in_transit_grows_by_one(
        self, resolved_model: ResolvedModelData,
    ) -> None:
        state = init_state(resolved_model)
        outcome = run_dispatch_lifecycle(
            _single_dispatch(), state, resolved_model, _make_period(),
        )
        assert len(outcome.state.in_transit) == 1

    def test_flow_event_emitted(
        self, resolved_model: ResolvedModelData,
    ) -> None:
        state = init_state(resolved_model)
        outcome = run_dispatch_lifecycle(
            _single_dispatch(), state, resolved_model, _make_period(),
        )
        assert len(outcome.flow_events) == 1

    def test_resource_marked_in_transit(
        self, resolved_model: ResolvedModelData,
    ) -> None:
        state = init_state(resolved_model)
        outcome = run_dispatch_lifecycle(
            _single_dispatch(), state, resolved_model, _make_period(0, "p0"),
        )
        in_transit = outcome.state.resources[
            outcome.state.resources["status"] == ResourceStatus.IN_TRANSIT.value
        ]
        assert len(in_transit) == 1
        # arrival_period was period_index + 1 = 1.
        assert in_transit.iloc[0]["available_at_period"] == 1
        # Resource was relocated to the target.
        assert in_transit.iloc[0]["current_facility_id"] == "s1"


class TestAssign:
    """``resource_id=None`` triggers auto-assignment from the available pool."""

    def test_null_resource_filled_with_truck_at_source(
        self, resolved_model: ResolvedModelData,
    ) -> None:
        state = init_state(resolved_model)
        outcome = run_dispatch_lifecycle(
            _single_dispatch(resource_id=None),
            state, resolved_model, _make_period(),
        )
        assigned = outcome.state.resources[
            outcome.state.resources["status"] == ResourceStatus.IN_TRANSIT.value
        ]
        assert len(assigned) == 1
        # The truck moved from d1 to s1 because we assigned a d1 truck and
        # then applied the dispatch.
        assert assigned.iloc[0]["current_facility_id"] == "s1"


class TestRejectInsufficientInventory:
    """Source without stock yields a rejection rather than touching state."""

    def test_state_unchanged(
        self, resolved_model: ResolvedModelData,
    ) -> None:
        state = init_state(resolved_model)
        empty_d1 = state.inventory.copy()
        empty_d1.loc[empty_d1["facility_id"] == "d1", "quantity"] = 0.0
        state = state.with_inventory(empty_d1)

        outcome = run_dispatch_lifecycle(
            _single_dispatch(), state, resolved_model, _make_period(),
        )
        assert outcome.state is state
        assert outcome.flow_events.empty
        assert outcome.state.in_transit.empty

    def test_rejection_reason_recorded(
        self, resolved_model: ResolvedModelData,
    ) -> None:
        state = init_state(resolved_model)
        empty_d1 = state.inventory.copy()
        empty_d1.loc[empty_d1["facility_id"] == "d1", "quantity"] = 0.0
        state = state.with_inventory(empty_d1)

        outcome = run_dispatch_lifecycle(
            _single_dispatch(), state, resolved_model, _make_period(),
        )
        assert len(outcome.rejected) == 1
        assert (
            outcome.rejected.iloc[0]["reason"]
            == RejectReason.INSUFFICIENT_INVENTORY.value
        )


class TestRejectInvalidArrival:
    """Arrival before the current period is rejected with INVALID_ARRIVAL."""

    def test_past_arrival_period(
        self, resolved_model: ResolvedModelData,
    ) -> None:
        state = init_state(resolved_model)
        bad = _single_dispatch()
        bad["arrival_period"] = -1  # before current period_index = 0

        outcome = run_dispatch_lifecycle(
            bad, state, resolved_model, _make_period(period_index=0),
        )
        assert len(outcome.rejected) == 1
        assert (
            outcome.rejected.iloc[0]["reason"]
            == RejectReason.INVALID_ARRIVAL.value
        )


class TestRejectionOrdering:
    """First failing rule sets the reason; later rules see it as already-rejected."""

    def test_invalid_arrival_beats_insufficient_inventory(
        self, resolved_model: ResolvedModelData,
    ) -> None:
        """A row that fails both rules reports the earlier rule's reason."""
        state = init_state(resolved_model)
        empty_d1 = state.inventory.copy()
        empty_d1.loc[empty_d1["facility_id"] == "d1", "quantity"] = 0.0
        state = state.with_inventory(empty_d1)

        bad = _single_dispatch()
        bad["arrival_period"] = -1  # invalid_arrival (rule 1)
        # Inventory at d1 is also 0 (insufficient_inventory, rule 5).

        outcome = run_dispatch_lifecycle(
            bad, state, resolved_model, _make_period(period_index=0),
        )
        assert (
            outcome.rejected.iloc[0]["reason"]
            == RejectReason.INVALID_ARRIVAL.value
        )


def _multi_dispatch(
    *,
    sources: list[str],
    targets: list[str],
    resource_id: object,
    period_index: int = 0,
) -> pd.DataFrame:
    """Multi-row dispatch frame sharing a resource_id, in DISPATCH_COLUMNS order."""
    n = len(sources)
    assert len(targets) == n
    return pd.DataFrame(
        {
            "source_id": sources,
            "target_id": targets,
            "commodity_category": ["working_bike"] * n,
            "quantity": [1.0] * n,
            "resource_id": [resource_id] * n,
            "modal_type": ["road"] * n,
            "arrival_period": [period_index + 1] * n,
        }
    )


class TestRejectUnavailableResourceRouteValidation:
    """Tests for the route-aware ``_reject_unavailable_resource`` validator.

    Single-row dispatches keep the strict ``(resource_id, source_id)`` per-row
    check.  Multi-row dispatches sharing the same non-null ``resource_id`` are
    treated as one logical route (e.g. a multi-stop PDP route emitted by
    ``RebalancerTask``).  Pickup-delivery rows do not form a physical
    traversal chain — a row's ``source_id`` is a pickup station, not the
    previous row's target — so the validator only checks that the resource
    itself is in the AVAILABLE pool, not the per-row source.
    """

    def test_route_with_truck_at_first_source_passes(
        self, resolved_model: ResolvedModelData,
    ) -> None:
        """Route whose first source is the truck location is accepted."""
        state = init_state(resolved_model)
        truck = "rebalancing_truck_d1_0"
        dispatches = _multi_dispatch(
            sources=["d1", "s1", "s2"],
            targets=["s1", "s2", "d1"],
            resource_id=truck,
        )
        dispatches["_reject_reason"] = None

        _reject_unavailable_resource(dispatches, state)

        assert dispatches["_reject_reason"].isna().all()

    def test_route_with_remote_pickups_passes(
        self, resolved_model: ResolvedModelData,
    ) -> None:
        """A pickup-delivery route is accepted even when no row's source equals the truck location.

        Pickup-delivery rows describe logical movements of bikes between
        stations (source = pickup, target = delivery).  They do not form a
        physical traversal chain rooted at the truck's location, so
        availability is the only check that applies.
        """
        state = init_state(resolved_model)
        truck = "rebalancing_truck_d1_0"  # truck is at d1
        dispatches = _multi_dispatch(
            sources=["s1", "s2"],
            targets=["s2", "d1"],
            resource_id=truck,
        )
        dispatches["_reject_reason"] = None

        _reject_unavailable_resource(dispatches, state)

        # Truck is available, so the whole route is accepted regardless of
        # individual source positions.
        assert dispatches["_reject_reason"].isna().all()

    def test_route_rejected_when_resource_id_unknown(
        self, resolved_model: ResolvedModelData,
    ) -> None:
        """Multi-row group whose resource_id is not in the AVAILABLE pool is fully rejected."""
        state = init_state(resolved_model)
        dispatches = _multi_dispatch(
            sources=["d1", "s1"],
            targets=["s1", "d1"],
            resource_id="ghost_truck_does_not_exist",
        )
        dispatches["_reject_reason"] = None

        _reject_unavailable_resource(dispatches, state)

        assert (
            dispatches["_reject_reason"]
            == RejectReason.NO_AVAILABLE_RESOURCE.value
        ).all()

    def test_single_row_unavailable_still_rejects(
        self, resolved_model: ResolvedModelData,
    ) -> None:
        """Single-row dispatches keep the strict per-row availability check."""
        state = init_state(resolved_model)
        truck = "rebalancing_truck_d1_0"  # truck is at d1, not s1
        dispatches = _multi_dispatch(
            sources=["s1"],
            targets=["s2"],
            resource_id=truck,
        )
        dispatches["_reject_reason"] = None

        _reject_unavailable_resource(dispatches, state)

        assert (
            dispatches["_reject_reason"].iloc[0]
            == RejectReason.NO_AVAILABLE_RESOURCE.value
        )

    def test_null_resource_id_skipped(
        self, resolved_model: ResolvedModelData,
    ) -> None:
        """Rows with null resource_id are not handled by this validator.

        Auto-assignment runs upstream in ``_assign_resources``.
        """
        state = init_state(resolved_model)
        dispatches = _multi_dispatch(
            sources=["s1", "s2"],
            targets=["s2", "d1"],
            resource_id=None,
        )
        dispatches["_reject_reason"] = None

        _reject_unavailable_resource(dispatches, state)

        assert dispatches["_reject_reason"].isna().all()
