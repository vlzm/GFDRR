"""OverflowRedirectPhase — redirect overflow inventory to nearest facility.

Extracted from ``built_in_phases.py`` for locality: the phase has its own
distance-matrix construction, vectorised argmin, and proportional
redistribution logic that is independent of the other 13 phases.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from gbp.consumers.simulator.distance import build_facility_distance_matrix
from gbp.consumers.simulator.phases import PhaseResult, Schedule

if TYPE_CHECKING:
    from gbp.consumers.simulator.state import PeriodRow, SimulationState
    from gbp.core.model import ResolvedModelData


class OverflowRedirectPhase:
    """Redirect overflow inventory to nearest facility with free capacity.

    ORDERING CONTRACT: Must run immediately after :class:`ArrivalsPhase`.
    ``ArrivalsPhase`` writes the period's incoming inventory into
    ``state.inventory``; this phase inspects the resulting inventory against
    ``operation_capacity[storage]`` and redirects any overflow per
    ``(facility_id, commodity_category)``.  Inserting other inventory-mutating
    phases between ``ArrivalsPhase`` and this one breaks redirect accounting.

    On the canonical historical replay where ``observed_flow.target_id`` is
    already post-redirect (spec Constraint 3), this phase is a no-op:
    capacity is never violated by construction, so no redirects are emitted.
    Treatment scenarios with inflated demand may legitimately trigger
    redirects.

    Per :data:`gbp.consumers.simulator.log.LOG_TABLES`, redirect events flow
    into ``simulation_redirected_flow_log`` with columns
    ``[period_index, period_id, phase_name, source_id, original_target_id,
    redirected_target_id, commodity_category, quantity]``.

    Facilities lacking an ``operation_capacity[storage]`` row are treated as
    unbounded: no overflow is recorded for them.  This mirrors
    :class:`DockCapacityPhase` semantics and keeps the canonical replay
    pipeline stable when a fixture lists only a subset of facilities in the
    storage attribute.

    Parameters
    ----------
    schedule
        Optional execution schedule.  Defaults to every period.
    """

    name: str = "OVERFLOW_REDIRECT"

    def __init__(self, schedule: Schedule | None = None) -> None:
        """Initialise with an optional schedule."""
        self._schedule = schedule or Schedule.every()
        self._distance_cache: tuple[list[str], np.ndarray] | None = None

    def should_run(self, period: PeriodRow) -> bool:
        """Delegate to schedule."""
        return self._schedule.should_run(period)

    # -- Distance matrix (cached) ----------------------------------------------

    def _get_distance_matrix(
        self,
        resolved: ResolvedModelData,
    ) -> tuple[list[str], np.ndarray]:
        """Return cached (facility_ids, N*N distance matrix).

        Delegates to :func:`build_facility_distance_matrix` on first call
        and caches the result.  The matrix depends only on ``resolved``
        (immutable across the simulation run).

        Parameters
        ----------
        resolved
            Resolved model data (facilities and optional distance_matrix).

        Returns
        -------
        tuple of (list[str], np.ndarray)
            Sorted facility ids and the square distance matrix in km.
        """
        if self._distance_cache is None:
            self._distance_cache = build_facility_distance_matrix(
                resolved.facilities, resolved.distance_matrix,
            )
        return self._distance_cache

    # -- Core logic ------------------------------------------------------------

    def execute(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
    ) -> PhaseResult:
        """Detect and redirect over-capacity inventory per commodity."""
        # 1. Read per-(facility, commodity) capacity from the storage attribute.
        if "operation_capacity" not in resolved.attributes:
            return PhaseResult.empty(state)
        cap_data = resolved.attributes.get("operation_capacity").data
        storage = cap_data.loc[
            cap_data["operation_type"] == "storage",
            ["facility_id", "commodity_category", "capacity"],
        ]
        if storage.empty:
            return PhaseResult.empty(state)

        # 2. Compute overflow and free capacity per (facility, commodity).
        # Facilities without a storage row are unbounded — no overflow there
        # and effectively infinite free capacity (mirrors DockCapacityPhase).
        merged = state.inventory.merge(
            storage,
            on=["facility_id", "commodity_category"],
            how="left",
        )
        bounded = merged["capacity"].notna()
        merged["overflow"] = 0.0
        merged["free_capacity"] = np.inf
        merged.loc[bounded, "overflow"] = (
            merged.loc[bounded, "quantity"] - merged.loc[bounded, "capacity"]
        ).clip(lower=0.0)
        merged.loc[bounded, "free_capacity"] = (
            merged.loc[bounded, "capacity"] - merged.loc[bounded, "quantity"]
        ).clip(lower=0.0)

        if not (merged["overflow"] > 0).any():
            return PhaseResult.empty(state)

        # 3. Get the sorted facility list and corresponding distance matrix
        # (cached after first call).  Distance source preference:
        # resolved.distance_matrix > Haversine fallback (ADR Sec. 7.6).
        facility_ids, distance = self._get_distance_matrix(resolved)

        # 4. Process per commodity_category.  Each commodity has its own
        # capacity table; redirects respect commodity isolation (a
        # working_bike overflow only goes to a station with free working_bike
        # capacity).
        redirect_rows: list[dict[str, object]] = []
        new_inventory = state.inventory.copy()

        for commodity in (
            merged.loc[merged["overflow"] > 0, "commodity_category"].astype(str).unique()
        ):
            slice_ = merged[merged["commodity_category"] == commodity]
            overflow_by_fac = (
                slice_.set_index("facility_id")["overflow"]
                .reindex(facility_ids, fill_value=0.0)
                .to_numpy(dtype=float)
            )
            free_by_fac = (
                slice_.set_index("facility_id")["free_capacity"]
                .reindex(facility_ids, fill_value=0.0)
                .to_numpy(dtype=float)
            )

            source_idx = np.flatnonzero(overflow_by_fac > 1e-9)
            if source_idx.size == 0:
                continue

            # 5. Vectorised nearest-with-capacity argmin.  D[source, target]
            # is restricted to candidate columns where free capacity > 0,
            # excluding the source itself.  np.argmin returns the first
            # minimum, which combined with the lexicographic facility_ids
            # ordering yields a deterministic lexicographic tie-break.
            distance_view = distance[source_idx, :]
            available = np.broadcast_to(
                free_by_fac > 1e-9,
                distance_view.shape,
            ).copy()
            available[np.arange(source_idx.size), source_idx] = False
            masked = np.where(available, distance_view, np.inf)

            if not np.isfinite(masked).any():
                continue
            winners = masked.argmin(axis=1)
            has_winner = np.isfinite(
                masked[np.arange(source_idx.size), winners],
            )
            if not has_winner.any():
                continue
            valid_sources = source_idx[has_winner]
            valid_winners = winners[has_winner]
            desired = overflow_by_fac[valid_sources]

            # 6. Resolve target oversubscription: aggregate desired demand
            # per winner, clamp to that winner's free capacity, distribute
            # the accepted amount proportionally back to each source.
            demand_df = pd.DataFrame(
                {
                    "winner": valid_winners,
                    "desired": desired,
                }
            )
            per_winner = demand_df.groupby("winner", as_index=False)["desired"].sum()
            per_winner["capacity"] = free_by_fac[per_winner["winner"].to_numpy()]
            per_winner["accepted"] = np.minimum(
                per_winner["desired"],
                per_winner["capacity"],
            )
            per_winner["scale"] = np.where(
                per_winner["desired"] > 1e-9,
                per_winner["accepted"] / per_winner["desired"],
                0.0,
            )
            scale_lookup = dict(
                zip(
                    per_winner["winner"].to_numpy(),
                    per_winner["scale"].to_numpy(),
                    strict=True,
                )
            )
            scaled_amounts = desired * np.array(
                [scale_lookup[w] for w in valid_winners],
                dtype=float,
            )

            # 7. Apply redirects.  The loop iterates over redirect events
            # (one per overflowing source × commodity), not per inventory
            # row — the inner hot path (argmin) was already vectorised.
            for src_idx, tgt_idx, amount in zip(
                valid_sources,
                valid_winners,
                scaled_amounts,
                strict=True,
            ):
                if amount <= 1e-9:
                    continue
                src_id = facility_ids[int(src_idx)]
                tgt_id = facility_ids[int(tgt_idx)]
                amount_f = float(amount)

                src_mask = (new_inventory["facility_id"] == src_id) & (
                    new_inventory["commodity_category"] == commodity
                )
                new_inventory.loc[src_mask, "quantity"] -= amount_f

                tgt_mask = (new_inventory["facility_id"] == tgt_id) & (
                    new_inventory["commodity_category"] == commodity
                )
                if tgt_mask.any():
                    new_inventory.loc[tgt_mask, "quantity"] += amount_f
                else:
                    new_inventory = pd.concat(
                        [
                            new_inventory,
                            pd.DataFrame(
                                [
                                    {
                                        "facility_id": tgt_id,
                                        "commodity_category": commodity,
                                        "quantity": amount_f,
                                    }
                                ]
                            ),
                        ],
                        ignore_index=True,
                    )

                redirect_rows.append(
                    {
                        "source_id": src_id,
                        "original_target_id": src_id,
                        "redirected_target_id": tgt_id,
                        "commodity_category": commodity,
                        "quantity": amount_f,
                    }
                )

        if not redirect_rows:
            return PhaseResult.empty(state)

        redirect_df = pd.DataFrame(
            redirect_rows,
            columns=[
                "source_id",
                "original_target_id",
                "redirected_target_id",
                "commodity_category",
                "quantity",
            ],
        )
        new_state = state.with_inventory(new_inventory.reset_index(drop=True))
        return PhaseResult(
            state=new_state,
            events={"redirected_flow": redirect_df},
        )
