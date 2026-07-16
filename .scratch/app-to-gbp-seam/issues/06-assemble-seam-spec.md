# 06 — Assemble the seam spec deliverable

Type: task
Status: resolved
Blocked by: 01, 02, 03, 04, 05

## Decision

Deliverable written to `docs/plans/app_to_gbp_seam.md`: the per-module verdict
table, the §12/§16 + CLAUDE.md before→after contract changes, the graduated fog
decisions (app/ stays flat; DiskBackend stays thinned), the testing decisions,
and the six-step commit order. No code moved.

## Question

Assemble the decisions from tickets 01–05 into the destination deliverable:
one hand-off spec at `docs/plans/app_to_gbp_seam.md`.

This ticket decides nothing new — it collects the resolved decisions into the
document a later execution effort follows. It is the last step because it
needs every other ticket resolved.

The spec must contain:
1. A per-module verdict table: for each non-UI piece in `app/` (the builder,
   the evaluation cluster, the run recipe, each `ui_shared` reduction) —
   **move / stay / split**, and the target `gbp` module.
2. The exact contract changes from ticket 05 (Notations §12/§16, CLAUDE.md),
   quoted as before → after.
3. The graduated fog decisions (app/ packaging, DiskBackend) if they were
   resolved along the way.
4. A suggested commit order for the execution effort, smallest safe steps
   first (e.g. metrics → evaluation → builder → reductions → docs).

Do not move any code. The output is the spec file only.
