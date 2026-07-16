<!-- wayfinder:map -->

# Map: app → gbp seam

## Destination

A **decided seam** between `app/` and `gbp/`. For every non-UI computation
that lives in `app/` today, the map produces a verdict — **move** into a named
`gbp` home / **stay** in app / **split** — plus the exact contract changes it
forces (Notations §12/§16, CLAUDE.md), assembled as one hand-off spec at
`docs/plans/app_to_gbp_seam.md`. The rule "app draws, gbp computes" already
exists (Notations §12); this effort relocates the computation, it does not
invent the rule. **No code is moved by this map** — it decides, a later effort
executes.

Reached when: no non-UI module in `app/` has an undecided home, and the
spec doc names each move with its target `gbp` module.

## Notes

- Domain: the GFDRR flow-graph repo; the one scenario is Citi Bike NYC. The
  simulator runs a base replay and a demand-forecast run.
- Scope was fixed with the user: **all non-UI logic** is in play. Stays in
  `app/`: `backend.py`, `api.py`, `api_client.py`, `main.py`, `views/*` (draw
  + IO). In play: `artifacts.py`, `eval_comparison.py`, `evaluate.py`, the
  dataframe reductions in `ui_shared.py`, the run recipe in `runner.py`.
- Every session consult: `/grilling`, `/domain-modeling`, and the
  `codebase-design`, `ousterhout-review`, `check-notations` skills.
- Hard constraints from CLAUDE.md, do not break: **vertical, not
  horizontal** — no domain-agnostic "service layer" over gbp for its own
  sake; **deep modules**; **gbp must never import app** (true today, keep it);
  documents are English by default.
- The boundary was mapped at charting time — the current app→gbp imports, the
  per-file classification, and the concrete leak sites are summarized in each
  ticket that needs them.

## Decisions so far

<!-- one line per resolved ticket: gist + link -->

- **01 [resolved]** — the artifact builder moves whole into one new module
  `gbp/artifacts.py` (builders + contracts `RunMeta`/`RUN_TABLE_SCHEMAS`/
  `METRICS` + save/load + `data/runs/` path policy). Journal-reshaping builders
  stay with it, not folded into `gbp/model`. [issues/01-artifact-builder-home.md]
- **02 [resolved]** — the two ad-hoc metrics (`panel_departed_mae`, the
  busy-share) move to `gbp/ml/metrics.py`; the comparison bookkeeping and the
  `evaluate.py` orchestration move to a new `gbp/ml/evaluation.py`, CLI
  `python -m gbp.ml.evaluation`. `app/eval_comparison.py` and `app/evaluate.py`
  are deleted, no app shell stays. [issues/02-evaluation-cluster.md]
- **03 [resolved]** — the whole run path moves into `gbp/consumers`:
  `RunRequest`, the fleet/depot defaults, `build_graph_data`, `run_scenario`,
  `run_and_save`. `app/runner.py` shrinks to the argparse CLI over the gbp
  path. This also breaks the `artifacts`↔`runner` cycle (both callers of
  `RunRequest` now sit in gbp). [issues/03-run-recipe.md]
- **04 [resolved]** — every `ui_shared` reduction stays in `app/` as a
  render-time helper; the artifact contract does not grow. They depend on
  UI-chosen inputs (period, commodity, the A/B pair, facility subset, top-N),
  which the builder cannot precompute, so §12's "must not compute anything the
  builder can precompute" is already satisfied. Only the `PANEL_VALUES` /
  `METRICS` import repoints to `gbp`. [issues/04-ui-shared-reductions.md]
- **05 [resolved]** — path references change, the rules do not: §12/§16 and the
  CLAUDE.md UI rule swap `app/artifacts.py` → `gbp/artifacts.py` and the run
  path module; "artifact builder" stays the canonical word, now living in gbp.
  No new §12 table rows (ticket 04 added none). [issues/05-canonical-contract-rewrite.md]

## Not yet specified

<!-- in-scope fog, graduates as tickets resolve -->

- **(graduated) app/ packaging after the move.** Resolved with 01+03: `app/`
  stays a flat sys.path folder. After `artifacts` and the run path leave, the
  only bare-name imports left are genuine app glue (`ui_shared`, `backend`,
  `api_client`, `views/*`); nothing forces `app/` to become an importable
  package. The moved code is reached as `from gbp import artifacts` /
  `from gbp.consumers... import ...`.
- **(graduated) DiskBackend after the builder moves.** Resolved with 01:
  `backend.py`'s DiskBackend stays but thins to a pass-through that delegates
  list/load/save to `gbp.artifacts`. It is the app-side seam that lets
  `api_client` swap disk for HTTP, so it does not vanish.

## Out of scope

<!-- ruled beyond this destination; never graduates -->

- **The FastAPI service** (`api.py`, `api_client.py`) — stays app-side; not
  extracted to a `service/` layer (user decision).
- **`backend.py`, `main.py`, `views/*`** — genuine draw + IO glue, stay in
  `app/`.
- **Any domain-agnostic "API/interface layer" over gbp** built for its own
  sake — forbidden by CLAUDE.md ("Vertical, not horizontal"). We move concrete
  domain code into concrete gbp modules, nothing more.
