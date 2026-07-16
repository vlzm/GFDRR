# 01 — Where does the artifact builder live in gbp?

Type: grilling
Status: resolved
Blocked by: —

## Decision

Move the whole builder into one new module `gbp/artifacts.py`: the table
builders, the contracts (`RunMeta`, `RUN_TABLE_SCHEMAS`, `METRICS`), save/load,
and the `data/runs/` path policy. The journal-reshaping builders stay with it,
they are not folded into `gbp/model`. Nothing of it stays app-side; `RunRequest`
in `save_scenario_run` comes from `gbp/consumers` (ticket 03), so no app import
remains. See `docs/plans/app_to_gbp_seam.md`.

## Question

`app/artifacts.py` (662 lines) is the "artifact builder" — the single largest
non-UI block in `app/`. Decide its new home in `gbp` and what, if anything,
stays app-side.

This is the keystone decision: it is named by the canonical docs, and the
`ui_shared` reductions ticket (04) depends on where the builder ends up.

What the file holds today:
- Table builders — pure reshaping of the flow journal: `build_arcs`
  (`artifacts.py:306`), `build_flow_totals` (`:365`), `build_facilities`
  (`:415`), `build_totals` (`:426`), `build_run_tables` (`:561`),
  `build_meta` (`:515`). These lean on `gbp.model` (`flows_with_measures`,
  `flows_to_panel`, `is_docking`) and `gbp.routing.Routes`, and re-encode the
  event-type vocabulary that already lives in `gbp.model`.
- Contracts: `RunMeta` / `RebalancingMeta` (pydantic), the pandera
  `RUN_TABLE_SCHEMAS`, the `Metric` / `METRICS` catalog.
- IO glue: `save_scenario_run` (`:613`), `save_run`, `load_run_table`,
  `load_run_meta`, `code_version` (git shell-out), the path helpers
  (`data_dir`, `runs_root`, `run_dir`, `table_path`, `next_free_run_name`).

Decisions to reach:
1. Does the whole builder move, or split — the journal-reshaping builders
   (`build_arcs`, `build_flow_totals`) next to `gbp/model/flows.py`, the
   contracts + save/load elsewhere?
2. What is the target module or package — e.g. `gbp/artifacts.py`, a
   `gbp/artifacts/` package, or fold into `gbp/model` + `gbp/consumers`?
3. What stays app-side (if anything) — e.g. the `data/runs/<name>/` path
   policy and the disk IO, or does gbp own those too?
4. Does moving the builder change who calls it (today `runner.py` and
   `backend.py`)?

Consult: `/domain-modeling` (this names a new gbp concept), `codebase-design`,
`ousterhout-review`. Do not edit code — record the decided home and interface.
