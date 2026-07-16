# 03 — Move the run recipe into gbp/consumers

Type: grilling
Status: resolved
Blocked by: —

## Decision

The whole run path moves into `gbp/consumers`: `RunRequest`, the fleet/depot
defaults (as one typed source), `build_graph_data`, `run_scenario`,
`run_and_save`. `app/runner.py` shrinks to the argparse CLI over the gbp path.
This breaks the `artifacts`↔`runner` cycle: both users of `RunRequest`
(`save_scenario_run` and the run path) now sit in gbp. See
`docs/plans/app_to_gbp_seam.md`.

## Question

`app/runner.py` (267 lines) is mostly orchestration + CLI, but it also holds
domain policy about how a scenario is configured. Decide what of that policy
moves into `gbp/consumers` and what stays as app/CLI glue.

What is domain policy vs glue today:
- Policy candidates: `RunRequest` (the pydantic run recipe), the fleet/depot
  defaults `DEFAULT_TRUCK_HOMES`, `DEPOT_IDS`, `DEFAULT_TRUCK_RATE`
  (`runner.py:36`), and the recipe methods `resolved_truck_homes`,
  `rebalancing_meta`, plus the forecast-name validation. These reach into
  simulator concepts (`RebalancingParams`, truck fleets) that `gbp/consumers`
  owns.
- Glue that stays: `build_graph_data`, `run_scenario`, `run_and_save`, the
  `argparse` `main()` — wiring `gbp.loaders` + `gbp.consumers.simulator` to
  the artifact save call.

Decisions to reach:
1. Does `RunRequest` (or its policy parts) move into `gbp/consumers`, and
   under what name?
2. Where do the fleet/depot defaults live — a typed object in
   `gbp/consumers`, so app and CLI share one source (CLAUDE.md: "no repeated
   recipes")?
3. What stays in `app/runner.py` — just the CLI parse + call?
4. Interaction with ticket 01: `run_and_save` calls the artifact builder; if
   the builder moves, does the whole save-a-run path land in gbp?

Consult: `/grilling`, `/domain-modeling`, `codebase-design`. Do not edit
code — record the split and target modules.
