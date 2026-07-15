# First run: the simulator on a synthetic scenario

This page takes you from a clean clone to your first flow journal in a few
minutes, without downloading any data. You write a scenario by hand — three
stations, three trips — run the real simulation engine on it through the
public interfaces of `gbp`, and read the two tables every run produces.
Then you change one line at a time and watch the failure events appear in
the journal.

Three words this page uses (exact contracts live in
[Notations.md](../../Notations.md)):

- A **period** is one step of simulated time; here every period is one hour.
- The **flow journal** is the run's event table: one row per event
  (`departed`, `arrived`, `redirected`, `lost`) of one bike's trip (§0, §1).
- **Inventory** is how many bikes each station holds at a moment (§2).

## What runs

The script below assembles the simulator's input contract, `ScenarioInputs`
([gbp/consumers/simulator/inputs.py](../../gbp/consumers/simulator/inputs.py)),
from three hand-written trips, and runs the real `Environment` on it with
the canonical phases — the same engine the full runs use; only the input
tables are synthetic. The run finishes in well under a second.

`ScenarioInputs` is the named list of the tables the simulator reads. It is
a Protocol: any object that carries those fields fits, so the script uses a
plain `types.SimpleNamespace`. The loader `ResolvedModelData` fills the same
fields from a month of real trips — that path is the
[minimal example](../scenarios/citibike.md#minimal-example) of the Citi Bike
scenario page.

Prerequisite: step 1 of [installation.md](installation.md); the base
package (`uv pip install -e .`) is enough — no data download. Save the
script as a file in the repository root (say `quickstart.py`) and run it
from there with `python quickstart.py` — the rest of the page edits one
line and reruns it.

## The scenario

Three stations sit in a row, about 140 metres apart. `s1` starts with five
bikes, `s2` with five, `s3` empty. In period 0 three riders head for `s3`:
two from `s1`, one from `s2`. Three lines near the top of the script define
all of that, and they are the lines to play with:

- `trips` — one row per historical trip: source, target, start period,
  end period.
- `capacities` — docks per station; a station not named here gets 10,000
  docks, so the limit never binds.
- `initial_inventory` — bikes on station before period 0; a station not
  named here starts empty.

## The script

<!-- code:quickstart-script -->
```python
import types

import pandas as pd

from gbp.consumers.simulator import Environment, EnvironmentConfig, canonical_phases
from gbp.logging import configure_logging
from gbp.model import flows as J
from gbp.routing import Routes

configure_logging()

# --- The scenario: edit these three and rerun. ------------------------------
# Each trip is (source, target, start period, end period).
trips = pd.DataFrame(
    [
        ("s1", "s3", 0, 1),
        ("s1", "s3", 0, 1),
        ("s2", "s3", 0, 1),
    ],
    columns=["source_id", "planned_target_id", "start_period", "planned_end_period"],
)
capacities = {}
initial_inventory = {"s1": 5, "s2": 5}

# --- The historical journal, built from the trips. --------------------------
trips["flow_id"] = "hist_" + trips.index.astype(str)
trips["commodity_category"] = "classic_bike"
history = pd.concat(
    [J.departed_events(trips), J.arrived_events(trips, trips["planned_end_period"])],
    ignore_index=True,
)
history = J.finalize_flows(J.stamp_history_ordering(history))

# --- The scenario inputs: the tables the simulator reads. -------------------
facilities = sorted(set(trips["source_id"]) | set(trips["planned_target_id"]))
geo = pd.DataFrame(
    {
        "facility_id": facilities,
        "lat": [40.0 + i * 1e-3 for i in range(len(facilities))],
        "lng": [-74.0 + i * 1e-3 for i in range(len(facilities))],
    }
)
periods = pd.DataFrame({"period_id": range(4)})
periods["start_timestamp"] = pd.Timestamp("2026-01-01") + periods["period_id"] * pd.Timedelta(
    hours=1
)
periods["end_timestamp"] = periods["start_timestamp"] + pd.Timedelta(hours=1)

inputs = types.SimpleNamespace(
    periods_df=periods,
    initial_inventory_df=pd.DataFrame(
        [
            {
                "facility_id": f,
                "commodity_category": "classic_bike",
                "quantity": initial_inventory.get(f, 0),
            }
            for f in facilities
        ]
    ),
    historical_demand_df=J.flows_to_departures(history),
    historical_od_matrix_df=J.flows_to_od_matrix(history),
    facilities_capacities_df=pd.DataFrame(
        [{"facility_id": f, "capacity": capacities.get(f, 10_000)} for f in facilities]
    ),
    facilities_geo_df=geo,
    routes=Routes(
        geo, "haversine", trip_speed_km_per_period=15.0, period_len=pd.Timedelta(hours=1)
    ),
)

# --- Run the real engine on it. ----------------------------------------------
config = EnvironmentConfig(
    phases=canonical_phases(),
    scenario_id="quickstart",
    number_of_periods=len(periods),
)
env = Environment(inputs, config)
state = env.run()

columns = [
    "flow_id", "event_type", "reason", "source_id",
    "planned_target_id", "realized_target_id", "period_id", "step_id",
]
print(env.simulated_flows_df[columns].to_string(index=False))
print()
print(state.state_inventory_df.to_string(index=False))
```

The first block turns the trips into the historical journal, with the same
functions the loader uses (`gbp.model.flows`): one `departed` and one
`arrived` row per trip, then `stamp_history_ordering` and `finalize_flows`
stamp the ordering columns and cast the canonical dtypes.

The second block fills `ScenarioInputs` field by field: the period grid
(four one-hour periods), the geography (the stations in a row — every
redirect leg is so short its travel time rounds to zero periods), the
inventory and the capacities from the two dicts, and the two tables the
phases actually consume — the demand and the OD matrix, both computed from
the historical journal (`flows_to_departures`, `flows_to_od_matrix`).

The third block runs the engine. `canonical_phases()` is the standard
period loop: dock earlier arrivals → form departures → dock same-period
arrivals. Validation is on by default, so after the last period the run
checks the run invariants I1–I5 and raises if one fails — a clean exit
means the run is consistent. The journal is `env.simulated_flows_df`, the
final inventory `state.state_inventory_df`.

## The base run

After two log lines (`periods_stepped`, `run_invariants_checked`), the
script prints the whole journal and the final inventory:

<!-- output:quickstart-base -->
```text
flow_id event_type reason source_id planned_target_id realized_target_id  period_id  step_id
sim_0_0   departed   <NA>        s1                s3               <NA>          0        0
sim_0_1   departed   <NA>        s1                s3               <NA>          0        0
sim_0_2   departed   <NA>        s2                s3               <NA>          0        0
sim_0_0    arrived   <NA>        s1                s3                 s3          1        1
sim_0_1    arrived   <NA>        s1                s3                 s3          1        1
sim_0_2    arrived   <NA>        s2                s3                 s3          1        1

facility_id commodity_category  quantity
         s1       classic_bike       3.0
         s2       classic_bike       4.0
         s3       classic_bike       3.0
```

Each trip is one flow (`sim_0_0`, `sim_0_1`, `sim_0_2`): the `departed` row
takes the bike off its station in period 0, the `arrived` row docks it at
`s3` in period 1. `step_id` numbers the run's inventory changes in order:
the three departures apply together as step 0, the three dockings as step 1
(Notations.md §0.1). The inventory confirms it: `s1` went from 5 bikes
to 3, `s2` from 5 to 4, `s3` from 0 to 3. Nothing binds — every station has
docks and bikes to spare — so the journal has only `departed` and `arrived`
rows.

## Change one line: a full dock forces redirects

Give `s3` a single dock and rerun:

<!-- code:quickstart-capacity-line -->
```python
capacities = {"s3": 1}
```

<!-- output:quickstart-capacity -->
```text
flow_id event_type    reason source_id planned_target_id realized_target_id  period_id  step_id
sim_0_0   departed      <NA>        s1                s3               <NA>          0        0
sim_0_1   departed      <NA>        s1                s3               <NA>          0        0
sim_0_2   departed      <NA>        s2                s3               <NA>          0        0
sim_0_0    arrived      <NA>        s1                s3                 s3          1        1
sim_0_1 redirected dock_full        s1                s3               <NA>          1        2
sim_0_1   departed      <NA>        s3                s2               <NA>          1        2
sim_0_1    arrived      <NA>        s3                s2                 s2          1        2
sim_0_2 redirected dock_full        s2                s3               <NA>          1        2
sim_0_2   departed      <NA>        s3                s2               <NA>          1        2
sim_0_2    arrived      <NA>        s3                s2                 s2          1        2

facility_id commodity_category  quantity
         s1       classic_bike       3.0
         s2       classic_bike       6.0
         s3       classic_bike       1.0
```

Three bikes reach `s3` in period 1, but only one fits. The first in the
batch (`sim_0_0`) takes the dock. The other two bounce, and each writes the
same chain (`departed` → `redirected` → `departed` → `arrived`): the journal
records `redirected` with `reason = dock_full` and no realized target, then
a new leg departs `s3` toward the nearest station with free docks, `s2`,
and docks there — within the same period, because the stations sit close
together. The inventory confirms it: `s3` holds exactly its capacity of 1,
and `s2` ends with 6 — five at the start, minus one departed, plus the two
redirected bikes.

## Change another line: an empty station loses its demand

Restore `capacities = {}`, cut `s1` down to one bike, and rerun:

<!-- code:quickstart-inventory-line -->
```python
initial_inventory = {"s1": 1, "s2": 5}
```

<!-- output:quickstart-inventory -->
```text
flow_id event_type   reason source_id planned_target_id realized_target_id  period_id  step_id
sim_0_0   departed     <NA>        s1                s3               <NA>          0        0
sim_0_1   departed     <NA>        s2                s3               <NA>          0        0
   <NA>       lost stockout        s1              <NA>               <NA>          0        0
sim_0_0    arrived     <NA>        s1                s3                 s3          1        1
sim_0_1    arrived     <NA>        s2                s3                 s3          1        1

facility_id commodity_category  quantity
         s1       classic_bike       0.0
         s2       classic_bike       4.0
         s3       classic_bike       2.0
```

Two riders want to leave `s1` in period 0, and only one bike is there. One
trip departs; the other demand is written as `lost` with
`reason = stockout`. Its `flow_id` is `<NA>` — the demand never became a
flow, no bike moved. `s1` ends the run empty.

Every table on this page is replayed against a fresh engine run by
[tests/test_docs_quickstart.py](../../tests/test_docs_quickstart.py): the
test executes the script exactly as printed here, replaces the same lines
the page tells you to replace, and compares the output line by line.

## Where to go next

- The same call chain on a month of real trips — the
  [minimal example](../scenarios/citibike.md#minimal-example) of the Citi
  Bike scenario page (needs the data download from
  [installation.md](installation.md), step 2).
- [ui.md](ui.md) — run the simulator on a month of real trips and browse
  the saved run in the web interface (same download).
- [Worked examples](../key-components/worked-examples.md) — the same kind
  of story for every mechanic: stockout, delayed redirect, redirect chain,
  truck rebalancing.
- [Notations.md](../../Notations.md) — the exact journal schema (§0), the
  step axis (§0.1), the four outcomes (§1).
