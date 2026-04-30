"""Dispatch lifecycle: assign resources, validate, apply to state.

This module owns the full journey of a dispatch from a Task's output to its
effect on the simulation state.  The public surface is intentionally small —
:class:`DispatchOutcome` and :func:`run_dispatch_lifecycle` — so callers do
not have to know the order or shape of the internal steps.

Internally, the lifecycle has three steps:

1. **Assign** — fill ``resource_id`` for dispatches that left it null, using
   resource availability at the source facility plus the optional
   ``resource_commodity_compatibility`` and ``resource_modal_compatibility``
   tables.  Sequential: each chosen resource is removed from the pool for
   later dispatches.
2. **Validate** — split into valid and rejected.  Five rejection rules run in
   a fixed order so that the *first* failing check sets ``reason``:
   ``invalid_arrival`` -> ``invalid_edge`` -> ``no_available_resource`` ->
   ``over_capacity`` -> ``insufficient_inventory``.
3. **Apply** — generate shipment ids, decrement source inventory, append to
   ``in_transit``, update affected resources to ``IN_TRANSIT``.

Tests target this module directly, without needing a Phase or a Task fake.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd

from gbp.consumers.simulator.inventory import apply_delta, to_inventory_delta
from gbp.consumers.simulator.log import RejectReason
from gbp.core.enums import ResourceStatus

if TYPE_CHECKING:
    from gbp.consumers.simulator.state import PeriodRow, SimulationState
    from gbp.core.model import ResolvedModelData


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DispatchOutcome:
    """Result of running dispatches through the full lifecycle.

    Attributes:
        state: Updated simulation state.  Equal to the input *state* whenever
            no dispatch was applied (empty input or every dispatch rejected).
        rejected: Dispatches that failed validation, with an extra ``reason``
            column.  Empty when nothing was rejected.
        flow_events: One row per applied dispatch.  Empty when nothing was
            applied.
    """

    state: SimulationState
    rejected: pd.DataFrame
    flow_events: pd.DataFrame


def run_dispatch_lifecycle(
    dispatches: pd.DataFrame,
    state: SimulationState,
    resolved: ResolvedModelData,
    period: PeriodRow,
) -> DispatchOutcome:
    """Run *dispatches* through assign -> validate -> apply.

    Args:
        dispatches: Task-produced dispatches.  Must follow ``DISPATCH_COLUMNS``
            (see :mod:`gbp.consumers.simulator.task`).  ``resource_id`` may be
            ``None`` for rows that should auto-assign.
        state: Current simulation state.
        resolved: Resolved model with edges, resources, and (optionally)
            compatibility tables.
        period: The period in which the dispatches fire.

    Returns:
        :class:`DispatchOutcome` — see attribute docs for state, rejected,
        and flow_events semantics.
    """
    if dispatches.empty:
        return DispatchOutcome(
            state=state,
            rejected=pd.DataFrame(),
            flow_events=pd.DataFrame(),
        )

    assigned = _assign_resources(dispatches, state, resolved)
    valid, rejected = _validate_dispatches(assigned, state, resolved, period)

    if valid.empty:
        return DispatchOutcome(
            state=state, rejected=rejected, flow_events=pd.DataFrame(),
        )

    new_state, flow_events = _apply_dispatches(state, valid, period)
    return DispatchOutcome(
        state=new_state, rejected=rejected, flow_events=flow_events,
    )


# ---------------------------------------------------------------------------
# Step 1: assign resources
# ---------------------------------------------------------------------------


def _assign_resources(
    dispatches: pd.DataFrame,
    state: SimulationState,
    resolved: ResolvedModelData,
) -> pd.DataFrame:
    """Fill missing ``resource_id`` using availability + compatibility.

    Sequential assignment: each chosen resource is excluded from the pool for
    later dispatches.  Dispatches whose ``resource_id`` was already set are
    passed through untouched.
    """
    dispatches = dispatches.copy()
    needs_resource = dispatches["resource_id"].isna()
    if not needs_resource.any():
        return dispatches

    available = state.resources[
        state.resources["status"] == ResourceStatus.AVAILABLE.value
    ].copy()

    compat_commodity: set[tuple[str, str]] | None = None
    if not resolved.resource_commodity_compatibility.empty:
        rcc = resolved.resource_commodity_compatibility
        compat_commodity = set(
            zip(rcc["resource_category"], rcc["commodity_category"], strict=False)
        )

    compat_modal: set[tuple[str, str]] | None = None
    if not resolved.resource_modal_compatibility.empty:
        rmc = resolved.resource_modal_compatibility
        compat_modal = set(
            zip(rmc["resource_category"], rmc["modal_type"], strict=False)
        )

    assigned: set[str] = set()
    for idx in dispatches.index[needs_resource]:
        row = dispatches.loc[idx]
        candidates = available[
            (available["current_facility_id"] == row["source_id"])
            & (~available["resource_id"].isin(assigned))
        ]

        if compat_commodity is not None:
            candidates = candidates[
                candidates["resource_category"].apply(
                    lambda rc, cc=row["commodity_category"]: (rc, cc)
                    in compat_commodity
                )
            ]

        if compat_modal is not None and pd.notna(row.get("modal_type")):
            candidates = candidates[
                candidates["resource_category"].apply(
                    lambda rc, mt=row["modal_type"]: (rc, mt) in compat_modal
                )
            ]

        if not candidates.empty:
            chosen = candidates.iloc[0]["resource_id"]
            dispatches.at[idx, "resource_id"] = chosen
            assigned.add(chosen)

    return dispatches


# ---------------------------------------------------------------------------
# Step 2: validate
# ---------------------------------------------------------------------------


def _validate_dispatches(
    dispatches: pd.DataFrame,
    state: SimulationState,
    resolved: ResolvedModelData,
    period: PeriodRow,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split *dispatches* into ``(valid, rejected)``.

    Rejection rules run in a fixed order; the first failing rule sets
    ``reason`` for that dispatch.  Subsequent rules ignore already-rejected
    rows.  Returns:

    - ``valid``     — rows that passed every rule, with the internal marker
      column dropped.
    - ``rejected``  — rows that failed at least one rule, with ``reason``.
    """
    dispatches = dispatches.copy()
    dispatches["_reject_reason"] = None

    _reject_invalid_arrival(dispatches, period)
    _reject_invalid_edge(dispatches, resolved)
    _reject_unavailable_resource(dispatches, state)
    _reject_over_capacity(dispatches, state, resolved)
    _reject_insufficient_inventory(dispatches, state)

    rejected_mask = dispatches["_reject_reason"].notna()
    rejected = dispatches[rejected_mask].rename(
        columns={"_reject_reason": "reason"},
    )
    valid = dispatches[~rejected_mask].drop(columns=["_reject_reason"])
    return valid, rejected


def _reject_invalid_arrival(
    dispatches: pd.DataFrame, period: PeriodRow,
) -> None:
    """Mark dispatches whose arrival_period is before the current period."""
    bad = dispatches["arrival_period"] < period.period_index
    dispatches.loc[bad & dispatches["_reject_reason"].isna(), "_reject_reason"] = (
        RejectReason.INVALID_ARRIVAL.value
    )


def _reject_invalid_edge(
    dispatches: pd.DataFrame, resolved: ResolvedModelData,
) -> None:
    """Mark dispatches referencing non-existent edges.

    No-op when the resolved model has no edge table — without an edge
    whitelist there is nothing to validate against, so dispatches pass
    this rule unchanged.
    """
    if resolved.edges.empty:
        return
    has_modal = dispatches["modal_type"].notna() & dispatches["_reject_reason"].isna()
    if not has_modal.any():
        return
    edge_keys = set(
        zip(
            resolved.edges["source_id"],
            resolved.edges["target_id"],
            resolved.edges["modal_type"],
            strict=False,
        )
    )
    sub = dispatches.loc[has_modal]
    bad_edge = pd.Series(
        [
            (s, t, m) not in edge_keys
            for s, t, m in zip(
                sub["source_id"], sub["target_id"], sub["modal_type"], strict=False,
            )
        ],
        index=sub.index,
    )
    dispatches.loc[bad_edge[bad_edge].index, "_reject_reason"] = (
        RejectReason.INVALID_EDGE.value
    )


def _reject_unavailable_resource(
    dispatches: pd.DataFrame, state: SimulationState,
) -> None:
    """Mark dispatches whose resource is not available at the source facility."""
    has_resource = dispatches["resource_id"].notna() & dispatches["_reject_reason"].isna()
    if not has_resource.any():
        return
    avail = state.resources[state.resources["status"] == ResourceStatus.AVAILABLE.value]
    avail_at_source = set(
        zip(avail["resource_id"], avail["current_facility_id"], strict=False)
    )
    sub = dispatches.loc[has_resource]
    bad_res = pd.Series(
        [
            (r, s) not in avail_at_source
            for r, s in zip(sub["resource_id"], sub["source_id"], strict=False)
        ],
        index=sub.index,
    )
    dispatches.loc[bad_res[bad_res].index, "_reject_reason"] = (
        RejectReason.NO_AVAILABLE_RESOURCE.value
    )


def _reject_over_capacity(
    dispatches: pd.DataFrame,
    state: SimulationState,
    resolved: ResolvedModelData,
) -> None:
    """Mark dispatches that exceed a resource's base capacity."""
    has_resource_valid = (
        dispatches["resource_id"].notna() & dispatches["_reject_reason"].isna()
    )
    if not has_resource_valid.any() or resolved.resource_categories.empty:
        return
    cap_map = resolved.resource_categories.set_index(
        "resource_category_id",
    )["base_capacity"]
    res_cat = state.resources.set_index("resource_id")["resource_category"]

    sub = dispatches[has_resource_valid].copy()
    sub["_res_cat"] = sub["resource_id"].map(res_cat)
    sub["_cap"] = sub["_res_cat"].map(cap_map)
    used = sub.groupby("resource_id")["quantity"].transform("sum")
    over = used > sub["_cap"]
    if over.any():
        over_idx = sub.index[over]
        not_yet = dispatches.loc[over_idx, "_reject_reason"].isna()
        dispatches.loc[not_yet[not_yet].index, "_reject_reason"] = (
            RejectReason.OVER_CAPACITY.value
        )


def _reject_insufficient_inventory(
    dispatches: pd.DataFrame, state: SimulationState,
) -> None:
    """Reject dispatches that would overdraw facility inventory.

    Sequential allocation: dispatches are processed in row order.  Earlier
    dispatches consume available inventory; later ones may be rejected if
    the remaining stock is insufficient.
    """
    pending_mask = dispatches["_reject_reason"].isna()
    if not pending_mask.any():
        return

    remaining: dict[tuple[str, str], float] = {}
    for _, row in state.inventory.iterrows():
        key = (str(row["facility_id"]), str(row["commodity_category"]))
        remaining[key] = float(row["quantity"])

    for idx in dispatches.index[pending_mask]:
        row = dispatches.loc[idx]
        key = (str(row["source_id"]), str(row["commodity_category"]))
        avail = remaining.get(key, 0.0)
        qty = float(row["quantity"])
        if qty > avail:
            dispatches.at[idx, "_reject_reason"] = (
                RejectReason.INSUFFICIENT_INVENTORY.value
            )
        else:
            remaining[key] = avail - qty


# ---------------------------------------------------------------------------
# Step 3: apply
# ---------------------------------------------------------------------------


def _apply_dispatches(
    state: SimulationState,
    dispatches: pd.DataFrame,
    period: PeriodRow,
) -> tuple[SimulationState, pd.DataFrame]:
    """Apply *dispatches* to *state*, returning ``(new_state, flow_events)``."""
    dispatches = dispatches.copy()

    # 1. Generate shipment IDs and stamp the departure period.
    dispatches["shipment_id"] = [
        f"shp_{period.period_index}_{i}" for i in range(len(dispatches))
    ]
    dispatches["departure_period"] = period.period_index

    # 2. Decrement source inventory.
    dec = to_inventory_delta(dispatches, facility_col="source_id")
    new_inv = apply_delta(state.inventory, dec, op="subtract")

    # 3. Append to in_transit.
    new_shipments = dispatches[
        [
            "shipment_id", "source_id", "target_id", "commodity_category",
            "quantity", "resource_id", "departure_period", "arrival_period",
        ]
    ].copy()
    frames = [f for f in (state.in_transit, new_shipments) if not f.empty]
    new_transit = (
        pd.concat(frames, ignore_index=True) if frames else state.in_transit.copy()
    )

    # 4. Update resources for dispatched ones.
    resources = state.resources.copy()
    dispatched_res = dispatches[dispatches["resource_id"].notna()]
    if not dispatched_res.empty:
        target_map = dispatched_res.drop_duplicates(
            subset=["resource_id"],
        ).set_index("resource_id")["target_id"]
        arrival_map = dispatched_res.drop_duplicates(
            subset=["resource_id"],
        ).set_index("resource_id")["arrival_period"]

        mask = resources["resource_id"].isin(target_map.index)
        resources.loc[mask, "status"] = ResourceStatus.IN_TRANSIT.value
        resources.loc[mask, "current_facility_id"] = (
            resources.loc[mask, "resource_id"].map(target_map)
        )
        resources.loc[mask, "available_at_period"] = (
            resources.loc[mask, "resource_id"].map(arrival_map)
        )

    # 5. Build flow events (one row per applied dispatch).
    flow_events = pd.DataFrame({
        "source_id": dispatches["source_id"].values,
        "target_id": dispatches["target_id"].values,
        "commodity_category": dispatches["commodity_category"].values,
        "modal_type": dispatches["modal_type"].values,
        "quantity": dispatches["quantity"].values,
        "resource_id": dispatches["resource_id"].values,
    })

    new_state = (
        state
        .with_inventory(new_inv)
        .with_in_transit(new_transit)
        .with_resources(resources)
    )
    return new_state, flow_events
