# How to debug an invariant violation (I1–I5)

Task: a run reports invariant violations — find which check broke and which
journal rows to look at.

## Where the violations are

`validate_run` (`gbp/consumers/simulator/validation.py`) checks every
finished run. The result reaches you in one of two ways:

- `run_sized_scenario(..., validate=True)` — the default, used by the
  notebooks and the tests — raises `RunInvariantError` with the full list;
- the terminal runner and the UI pass `validate=False`: the run is saved
  anyway, the list goes into `meta.json` under `violations`, and the runner
  prints the count (`Invariant violations: N`).

For a saved run, start here:

```bash
python - <<'PY'
import json
print(json.load(open("data/runs/<run_name>/meta.json"))["violations"])
PY
```

## What each message means

Every violation string starts with the invariant id and names the exact
place:

| Message starts with | The rule that broke | Check |
|---|---|---|
| `I1 <station>/<bike type> p<period>` | demand = departed + lost (stockout), per period, station, bike type | `check_demand_split`, `gbp/model/flows.py` |
| `I2 flow closure: N flows ...` | every flow due by the run's end closes with exactly one terminal event: an `arrived`, or a `lost` with `dock_full` | `check_flow_closure`, `gbp/model/flows.py` |
| `I3 <station>/<bike type>` | the live final inventory equals the inventory recomputed from the journal | `gbp/consumers/simulator/validation.py` |
| `I4 conservation` | Σ initial = Σ final inventory + Σ lost (dock_full) + Σ in transit | `gbp/consumers/simulator/validation.py` |
| `I5 step <step_id> <station>/<bike type>` | no inventory step takes a station below zero | `gbp/consumers/simulator/validation.py` |

The same list can also carry journal schema errors
(`check_journal_schema`), reported before I1–I5.

## Find the rows

I1 and I5 name a station and a period or step. Filter the saved journal to
that place and read the events in step order:

```python
import pandas as pd

flows = pd.read_parquet("data/runs/<run_name>/flows.parquet")
rows = flows[
    (flows["period_id"] == 17)
    & ((flows["source_id"] == "s1") | (flows["realized_target_id"] == "s1"))
]
print(rows.sort_values("step_id").to_string())
```

For I5, recompute the inventory around every step with
`inventory_at_moments` (`gbp/model/flows.py`): one row per step and station
with `inventory_before` and `inventory_after`. The initial inventory is the
panel's period-0 `quantity_sop`:

```python
from gbp.model import inventory_at_moments

panel = pd.read_parquet("data/runs/<run_name>/panel.parquet")
initial = panel.loc[
    panel["period_id"] == 0, ["facility_id", "commodity_category", "quantity_sop"]
].rename(columns={"quantity_sop": "quantity"})
moments = inventory_at_moments(flows, initial)
print(moments[moments["inventory_after"] < 0])
```

For I2 the message carries a count, not a name: group the journal by
`flow_id` and look for flows whose last event is a `departed` or
`redirected` with `planned_end_period` before the run's end — those are the
flows that never closed.

## What usually broke

Every invariant holds in an exact replay (scale 1.0, canonical phases); the
checks only start to matter above the baseline or after a code change — so
the first suspect is the change you just made. A new phase that stamps
events under a wrong `phase_rank`, so two batches that needed ordering
collapse into one step, shows up as I5 (the step contract,
[Notations.md §0.1](../../Notations.md)). A change in how departures or
losses are written shows up as I1 — the journal no longer matches the demand
table. A bike that leaves the system without a `lost` row shows up as I4.

Expected result: after the fix, the same run command prints
`Invariant violations: 0`, and `violations` in `meta.json` is empty.
