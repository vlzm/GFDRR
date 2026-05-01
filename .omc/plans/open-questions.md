# Open Questions Log

Tracks open questions, deferred decisions, and spec interpretations across all
plans. Append new entries; do not rewrite history.

## rebalancer-pdp-task — 2026-05-01

- [ ] **`modal_type` literal for trucks** — Spec uses `"truck"`; only enum
      value is `ModalType.ROAD = "road"` (`gbp/core/enums.py:24`). Plan
      resolves to `ModalType.ROAD.value`. Confirm or extend the enum.
- [ ] **Commodity-category selection per multi-stop pair** — Spec is silent
      on how to pick a category when a station holds multiple. Plan picks
      dominant by quantity, alphabetic tie-break. Confirm or change to "one
      row per category."
- [SUPERSEDED] ~~**`period_duration_hours` parameter source** — Plan exposes it on
      `RebalancerTask.__init__` defaulting to 1.0; alternative is to read
      from `resolved.periods` automatically. Confirm.~~
      → Resolved in iteration 2. See plan §8 item 3 (authoritative):
      default flipped to `None`; constructor reads period length from
      `resolved.periods` at first call.
- [ ] **`truck_speed_kmh` default** — Plan uses 30.0 km/h matching
      `GraphLoaderConfig.default_speed_kmh`. Confirm or set per-experiment.
- [SUPERSEDED] ~~**`gbp/rebalancer/{dataloader,pipeline}.py` deletion** — Plan marks them
      DEPRECATED via comment, defers physical deletion to a cleanup PR.
      Confirm or instruct to delete in the same PR.~~
      → Resolved in iteration 2. See plan §8 item 5 (authoritative):
      both files are deleted in this PR (Step 3 + Step 6) with
      salvage-into-`tasks/rebalancer.py`-first flow.
- [ ] **Truck infrastructure path (extend production loader vs mock-only)** —
      Plan picks Option A (mock-only seed). Promote to production loader
      after the experiment validates the configuration?

## rebalancer-pdp-task — 2026-05-01 (iteration 2 risks)

- [ ] **R8 — multi-stop validator collision** — `dispatch_lifecycle.py:268-282`
      builds `avail_at_source = {(resource_id, current_facility_id)}` and
      rejects every row whose `(resource_id, source_id)` is not in that
      set. Multi-stop routes break this because the same `resource_id` is
      paired with multiple `source_id`s; only the depot-row passes. Plan
      picks option (b): patch `_reject_unavailable_resource` to treat
      multi-row groups as route dispatches. Confirm during executor
      review or instruct planner to switch to option (a) (single-pair v1)
      or option (c) (`route_id` contract change).
- [ ] **R9 — `RebalancerTask` early-exit ordering** — Plan places the
      AVAILABLE-truck check as the first guard at the top of
      `RebalancerTask.run()`, before any pandas/solver work
      (Step 3 algorithm step 0). Confirm or instruct to log a debug event
      when the early exit short-circuits.
- [ ] **R10 — `operation_capacity` missing** — Plan includes a fallback
      `inventory_capacity = state.inventory.groupby("facility_id")["quantity"].max() * 2`
      when `resolved.attributes` lacks `operation_capacity` or its
      storage slice is empty (Step 3 algorithm step 1). Pattern matches
      `built_in_phases.py:720-723`. Confirm fallback formula or replace.
- [ ] **R11 — OR-Tools nondeterminism** — Plan adds
      `pdp_random_seed: int = 42` to `RebalancerTask.__init__`, threaded
      into `solve_pdp` → `RoutingSearchParameters`. Both baseline and
      treatment notebook runs use the same seed for fair comparison.
      Confirm seed value and naming.
