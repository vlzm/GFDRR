# 03 — Move the artifact builder into gbp/artifacts.py

**What to build:** the whole run-artifact builder moves from `app/artifacts.py`
into one new module `gbp/artifacts.py`: the table builders (`build_arcs`,
`build_flow_totals`, `build_facilities`, `build_totals`, `build_run_tables`),
the contracts (`RunMeta`, `RebalancingMeta`, `RUN_TABLE_SCHEMAS` and the
per-table pandera schemas, `Metric`/`METRICS`, `RUN_TABLES`, `PANEL_VALUES`,
`PANEL_FLOW_VALUES`), the save/load functions, and the `data/runs/` path policy.
The journal-reshaping builders stay with the contracts and save/load in the one
module — they are not folded into `gbp/model`. `save_scenario_run` takes
`RunRequest` from `gbp/consumers` (available after ticket 02), so no `app`
import remains in the builder. Every app caller — `ui_shared`, `backend`,
`api`, the `runner` CLI, and the views — reads the builder as
`from gbp import artifacts`. `DiskBackend` keeps its list/load/save methods but
thins to a pass-through that delegates to `gbp.artifacts`. A saved run
round-trips (build → save → load) to the same tables and `meta.json` as before.

**Blocked by:** 02 — needs `RunRequest` in `gbp/consumers` so the builder does
not import back through `app/`.

**Status:** ready-for-agent

- [ ] `gbp/artifacts.py` holds the builders, the contracts, save/load, and the `data/runs/` path helpers; `app/artifacts.py` is gone.
- [ ] `save_scenario_run` imports `RunRequest` from `gbp/consumers`, not from `app`.
- [ ] All app callers (`ui_shared`, `backend`, `api`, the `runner` CLI, `views/*`) import the builder from `gbp`.
- [ ] `DiskBackend` stays in `app/backend.py` but delegates list/load/save to `gbp.artifacts`.
- [ ] A saved run artifact round-trips to identical tables and `meta.json` (golden comparison green).
- [ ] `grep -rn "import app" gbp/` is empty.
- [ ] The artifact round-trip tests point at `gbp.artifacts`, assertions unchanged; the suite is green.
