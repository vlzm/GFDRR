# 02 — Move the run path into gbp/consumers/run.py

**What to build:** the whole "configure → run → save" path moves out of
`app/runner.py` into `gbp/consumers`. The run recipe (`RunRequest`), the
fleet/depot defaults as one typed source, and the orchestration
(`build_graph_data`, `run_scenario`, `run_and_save`) become `gbp` code with no
UI. `app/runner.py` shrinks to the argparse `main()` that parses flags and
calls the `gbp` run path, so `python app/runner.py --help` and a full CLI run
keep working. The API's Run page and the terminal both drive the same `gbp`
function. This move also breaks the `artifacts`↔`runner` import cycle:
`save_scenario_run` will take `RunRequest` from `gbp/consumers`, so nothing in
`gbp` imports back through `app/`.

The fleet/depot defaults (`DEFAULT_TRUCK_HOMES`, `DEFAULT_N_DEPOTS`, `DEPOT_IDS`,
`DEFAULT_TRUCK_CAPACITY_BIKES`, `DEFAULT_TRUCK_RATE`) are used by both the run
recipe and `build_graph_data`; they become one source both read, not two copies.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] `RunRequest` lives in `gbp/consumers` (e.g. `gbp/consumers/run.py`), with its forecast-name validation intact.
- [ ] The fleet/depot defaults are one typed source in `gbp/consumers`, read by both the recipe and `build_graph_data`.
- [ ] `build_graph_data`, `run_scenario`, and `run_and_save` live in `gbp/consumers`.
- [ ] `app/runner.py` holds only the argparse CLI over the `gbp` path; `python app/runner.py --run-name ... ` runs and saves a scenario as before.
- [ ] `app/api.py` starts runs through the `gbp` run path (the Run page still works).
- [ ] `grep -rn "import app" gbp/` is empty; the `artifacts`↔`runner` cycle no longer exists.
- [ ] The recipe/orchestration tests move to a `gbp/consumers` test, assertions unchanged; the suite is green.
