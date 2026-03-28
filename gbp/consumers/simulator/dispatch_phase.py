"""DispatchPhase: delegates to a Task, validates and applies dispatches.

This is the bridge between domain-specific Tasks (which produce dispatches)
and the simulation state (which tracks inventory, in-transit, resources).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from gbp.consumers.simulator.log import RejectReason
from gbp.consumers.simulator.phases import PhaseResult, Schedule
from gbp.consumers.simulator.state import INVENTORY_COLUMNS
from gbp.consumers.simulator.task import Task
from gbp.core.enums import ResourceStatus

if TYPE_CHECKING:
    from gbp.consumers.simulator.state import PeriodRow, SimulationState
    from gbp.core.model import ResolvedModelData


class DispatchPhase:
    """Phase that delegates to a Task and applies dispatches to state.

    Orchestration flow:
    1. ``Task.run()`` produces dispatches DataFrame.
    2. ``_validate_dispatches`` splits into valid + rejected.
    3. ``_apply_dispatches`` updates inventory, in_transit, resources.

    Attributes:
        name: Phase name, auto-derived as ``DISPATCH_{task.name}``.
    """

    def __init__(
        self,
        task: Task,
        schedule: Schedule | None = None,
    ) -> None:
        """Initialise with a task and optional schedule."""
        self.name: str = f"DISPATCH_{task.name}"
        self._task = task
        self._schedule = schedule or Schedule.every()

    def should_run(self, period: PeriodRow) -> bool:
        """Delegate to schedule."""
        return self._schedule.should_run(period)

    def execute(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
    ) -> PhaseResult:
        """Run the task, validate dispatches, apply to state."""
        dispatches = self._task.run(state, resolved, period)

        if dispatches.empty:
            return PhaseResult.empty(state)

        # Auto-assign resources where resource_id is null
        dispatches = self._auto_assign_resources(dispatches, state, resolved)

        valid, rejected = self._validate_dispatches(
            dispatches, state, resolved, period
        )

        if valid.empty:
            return PhaseResult(
                state=state,
                flow_events=pd.DataFrame(),
                unmet_demand=pd.DataFrame(),
                rejected_dispatches=rejected,
            )

        new_state, flow_events = self._apply_dispatches(state, valid, period)
        return PhaseResult(
            state=new_state,
            flow_events=flow_events,
            unmet_demand=pd.DataFrame(),
            rejected_dispatches=rejected,
        )

    # -- Validation ------------------------------------------------------------

    def _validate_dispatches(
        self,
        dispatches: pd.DataFrame,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Validate dispatches against current state and model.

        Returns (valid_dispatches, rejected_dispatches).
        """
        dispatches = dispatches.copy()
        dispatches["_reject_reason"] = None

        # 1. Arrival period sanity
        bad_arrival = dispatches["arrival_period"] < period.period_index
        dispatches.loc[bad_arrival, "_reject_reason"] = (
            RejectReason.INVALID_ARRIVAL.value
        )

        # 2. Edge existence (skip rows with null modal_type)
        if resolved.edges is not None:
            has_modal = dispatches["modal_type"].notna()
            if has_modal.any():
                edge_keys = set(
                    zip(
                        resolved.edges["source_id"],
                        resolved.edges["target_id"],
                        resolved.edges["modal_type"],
                        strict=False,
                    )
                )
                dispatch_keys = list(
                    zip(
                        dispatches.loc[has_modal, "source_id"],
                        dispatches.loc[has_modal, "target_id"],
                        dispatches.loc[has_modal, "modal_type"],
                        strict=False,
                    )
                )
                bad_edge = pd.Series(
                    [k not in edge_keys for k in dispatch_keys],
                    index=dispatches.loc[has_modal].index,
                )
                dispatches.loc[
                    bad_edge[bad_edge].index, "_reject_reason"
                ] = RejectReason.INVALID_EDGE.value

        # 3. Resource availability (only for rows with explicit resource_id)
        has_resource = dispatches["resource_id"].notna()
        if has_resource.any():
            avail = state.resources[
                state.resources["status"] == ResourceStatus.AVAILABLE.value
            ]
            avail_at_source = set(
                zip(avail["resource_id"], avail["current_facility_id"], strict=False)
            )
            dispatch_res = list(
                zip(
                    dispatches.loc[has_resource, "resource_id"],
                    dispatches.loc[has_resource, "source_id"],
                    strict=False,
                )
            )
            bad_res = pd.Series(
                [k not in avail_at_source for k in dispatch_res],
                index=dispatches.loc[has_resource].index,
            )
            not_yet_rejected = dispatches.loc[bad_res[bad_res].index, "_reject_reason"].isna()
            dispatches.loc[
                not_yet_rejected[not_yet_rejected].index, "_reject_reason"
            ] = RejectReason.NO_AVAILABLE_RESOURCE.value

        # 4. Capacity check per resource
        has_resource_valid = (
            dispatches["resource_id"].notna() & dispatches["_reject_reason"].isna()
        )
        if has_resource_valid.any() and resolved.resource_categories is not None:
            cap_map = resolved.resource_categories.set_index(
                "resource_category_id"
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
                dispatches.loc[
                    not_yet[not_yet].index, "_reject_reason"
                ] = RejectReason.OVER_CAPACITY.value

        # 5. Inventory sufficiency (check aggregated dispatches vs available)
        pending = dispatches[dispatches["_reject_reason"].isna()]
        if not pending.empty:
            dispatched_qty = (
                pending
                .groupby(["source_id", "commodity_category"], as_index=False)["quantity"]
                .sum()
                .rename(columns={"source_id": "facility_id", "quantity": "dispatched"})
            )
            inv_check = dispatched_qty.merge(
                state.inventory[["facility_id", "commodity_category", "quantity"]],
                on=["facility_id", "commodity_category"],
                how="left",
            )
            inv_check["quantity"] = inv_check["quantity"].fillna(0.0)
            over_inv = inv_check[inv_check["dispatched"] > inv_check["quantity"]]

            if not over_inv.empty:
                bad_pairs = set(
                    zip(
                        over_inv["facility_id"],
                        over_inv["commodity_category"],
                        strict=False,
                    )
                )
                match = dispatches.apply(
                    lambda r: (r["source_id"], r["commodity_category"]) in bad_pairs,
                    axis=1,
                )
                not_yet = match & dispatches["_reject_reason"].isna()
                dispatches.loc[
                    not_yet, "_reject_reason"
                ] = RejectReason.INSUFFICIENT_INVENTORY.value

        # Split
        rejected_mask = dispatches["_reject_reason"].notna()
        rejected = dispatches[rejected_mask].rename(
            columns={"_reject_reason": "reason"}
        )
        valid = dispatches[~rejected_mask].drop(columns=["_reject_reason"])

        return valid, rejected

    # -- Application -----------------------------------------------------------

    def _apply_dispatches(
        self,
        state: SimulationState,
        dispatches: pd.DataFrame,
        period: PeriodRow,
    ) -> tuple[SimulationState, pd.DataFrame]:
        """Apply valid dispatches to state.  Returns (new_state, flow_events)."""
        dispatches = dispatches.copy()

        # 1. Generate shipment IDs
        dispatches["shipment_id"] = [
            f"shp_{period.period_index}_{i}" for i in range(len(dispatches))
        ]
        dispatches["departure_period"] = period.period_index

        # 2. Decrement source inventory
        dec = (
            dispatches
            .groupby(["source_id", "commodity_category"], as_index=False)["quantity"]
            .sum()
            .rename(columns={"source_id": "facility_id", "quantity": "dec_qty"})
        )
        new_inv = state.inventory.merge(
            dec,
            on=["facility_id", "commodity_category"],
            how="left",
        )
        new_inv["dec_qty"] = new_inv["dec_qty"].fillna(0.0)
        new_inv["quantity"] = new_inv["quantity"] - new_inv["dec_qty"]
        new_inv = new_inv[INVENTORY_COLUMNS].copy()

        # 3. Append to in_transit
        new_shipments = dispatches[
            [
                "shipment_id", "source_id", "target_id", "commodity_category",
                "quantity", "resource_id", "departure_period", "arrival_period",
            ]
        ].copy()
        frames = [state.in_transit, new_shipments]
        frames = [f for f in frames if not f.empty]
        new_transit = pd.concat(frames, ignore_index=True) if frames else state.in_transit.copy()

        # 4. Update resources for dispatched ones
        resources = state.resources.copy()
        dispatched_res = dispatches[dispatches["resource_id"].notna()]
        if not dispatched_res.empty:
            # Build maps: resource_id -> target_id, resource_id -> arrival_period
            target_map = dispatched_res.drop_duplicates(
                subset=["resource_id"]
            ).set_index("resource_id")["target_id"]
            arrival_map = dispatched_res.drop_duplicates(
                subset=["resource_id"]
            ).set_index("resource_id")["arrival_period"]

            mask = resources["resource_id"].isin(target_map.index)
            resources.loc[mask, "status"] = ResourceStatus.IN_TRANSIT.value
            resources.loc[mask, "current_facility_id"] = (
                resources.loc[mask, "resource_id"].map(target_map)
            )
            resources.loc[mask, "available_at_period"] = (
                resources.loc[mask, "resource_id"].map(arrival_map)
            )

        # 5. Build flow events
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

    # -- Auto-assign -----------------------------------------------------------

    def _auto_assign_resources(
        self,
        dispatches: pd.DataFrame,
        state: SimulationState,
        resolved: ResolvedModelData,
    ) -> pd.DataFrame:
        """Assign available resources to dispatches with null resource_id.

        Sequential assignment: each assigned resource is removed from the
        available pool for subsequent dispatches.
        """
        dispatches = dispatches.copy()
        needs_resource = dispatches["resource_id"].isna()

        if not needs_resource.any():
            return dispatches

        # Build pool of available resources
        available = state.resources[
            state.resources["status"] == ResourceStatus.AVAILABLE.value
        ].copy()

        # Compatibility filters
        compat_commodity: set[tuple[str, str]] | None = None
        if (
            resolved.resource_commodity_compatibility is not None
            and not resolved.resource_commodity_compatibility.empty
        ):
            rcc = resolved.resource_commodity_compatibility
            compat_commodity = set(
                zip(rcc["resource_category"], rcc["commodity_category"], strict=False)
            )

        compat_modal: set[tuple[str, str]] | None = None
        if (
            resolved.resource_modal_compatibility is not None
            and not resolved.resource_modal_compatibility.empty
        ):
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
