# First run: the simulator on a synthetic scenario

This page takes you from a clean clone to your first flow journal in a few
minutes, without downloading any data. You run the real simulation engine on a
scenario built from a few hand-written trips and read the two tables every run
produces: the flow journal and the station inventory.

Three words this page uses (exact contracts live in
[Notations.md](../../Notations.md)):

- A **period** is one step of simulated time; here every period is one hour.
- The **flow journal** is the run's event table: one row per event
  (`departed`, `arrived`, `redirected`, `lost`) of one bike's trip (§0, §1).
- **Inventory** is how many bikes each station holds at a moment (§2).

## What runs

The builders in [tests/scenarios.py](../../tests/scenarios.py) assemble the
simulator's input contract, `ScenarioInputs`, from a handful of hand-written
trips: the period grid, the initial inventory, the dock capacities, the
geography. `run()` then runs the real `Environment` on it with the canonical
phases — the same engine the full runs use; only the input tables are
synthetic. Each scenario finishes in well under a second.

Prerequisite: step 1 of [installation.md](installation.md); the base
package (`uv pip install -e .`) is enough — no data download. Run everything
from the repository root.

## Run 1: one bike, one trip

`single_trip` is the smallest non-empty run: station `s1` starts with five
bikes, and one trip `s1 → s2` departs in period 0 and arrives in period 1.

```bash
python - <<'PY'
from gbp.logging import configure_logging
from tests.scenarios import run, single_trip

configure_logging()
journal, state = run(single_trip())

columns = [
    "flow_id", "event_type", "reason", "source_id",
    "planned_target_id", "realized_target_id", "period_id", "step_id",
]
print(journal[columns].to_string(index=False))
print(state.state_inventory_df.to_string(index=False))
PY
```

After one log line (`periods_stepped`), the script prints the flow journal:

| flow_id | event_type | reason | source_id | planned_target_id | realized_target_id | period_id | step_id |
|---|---|---|---|---|---|---|---|
| sim_0_0 | departed | NA | s1 | s2 | NA | 0 | 0 |
| sim_0_0 | arrived | NA | s1 | s2 | s2 | 1 | 1 |

and the final inventory:

| facility_id | commodity_category | quantity |
|---|---|---|
| s1 | classic_bike | 4 |
| s2 | classic_bike | 1 |

Both rows belong to the same flow `sim_0_0` — one bike's trip. The
`departed` row takes the bike off `s1` in period 0; the `arrived` row docks
it at `s2` in period 1. The inventory confirms it: `s1` went from 5 bikes to
4, `s2` from 0 to 1. `step_id` numbers the run's inventory changes in order:
the departure is step 0, the docking is step 1 (Notations.md §0.1).

Note: the console prints missing values as `<NA>` and quantities as floats
(`4.0`); this page writes `NA` and `4`, like the [worked examples](../key-components/worked-examples.md).

## Run 2: a full dock forces a redirect

`overflow` sends six bikes to `s3` in the same period, but the docks of `s3`
hold only two. Two bikes dock; the other four are redirected to the nearest
station with free docks, `s2`.

```bash
python - <<'PY'
from gbp.logging import configure_logging
from tests.scenarios import overflow, run

configure_logging()
journal, state = run(overflow())

bounced_id = journal.loc[journal["event_type"] == "redirected", "flow_id"].iloc[0]
bounced = journal[journal["flow_id"] == bounced_id]
columns = [
    "flow_id", "event_type", "reason", "source_id",
    "planned_target_id", "realized_target_id", "period_id", "step_id",
]
print(bounced[columns].to_string(index=False))
print(state.state_inventory_df.to_string(index=False))
PY
```

The script prints one redirected flow, row by row
(`departed` → `redirected` → `departed` → `arrived`):

| flow_id | event_type | reason | source_id | planned_target_id | realized_target_id | period_id | step_id |
|---|---|---|---|---|---|---|---|
| sim_0_2 | departed | NA | s1 | s3 | NA | 0 | 0 |
| sim_0_2 | redirected | dock_full | s1 | s3 | NA | 1 | 2 |
| sim_0_2 | departed | NA | s3 | s2 | NA | 1 | 2 |
| sim_0_2 | arrived | NA | s3 | s2 | s2 | 1 | 2 |

The bike departs `s1` toward `s3`. On arrival the dock is full, so the
journal writes `redirected` with `reason = dock_full` and no realized target.
A new leg departs `s3` toward `s2` and docks there — within the same period,
because the synthetic stations sit close together. The final inventory
confirms it: `s3` holds exactly its capacity of 2, and the four redirected
bikes raised `s2` from 50 − 3 departed to 51.

## Where to go next

- [ui.md](ui.md) — run the simulator on a month of real trips and browse
  the saved run in the web interface (needs the data download from
  [installation.md](installation.md), step 2).
- [Worked examples](../key-components/worked-examples.md) — the same kind of story for
  every mechanic: stockout, delayed redirect, redirect chain, truck
  rebalancing. Each table there is checked against a fresh engine run by
  `tests/test_docs_scenarios.py`.
- [Notations.md](../../Notations.md) — the exact journal schema (§0), the step
  axis (§0.1), the four outcomes (§1).
- The other builders in `tests/scenarios.py` (`stockout`, `network_full`,
  ...) run the same way: pass them to `run()`.
