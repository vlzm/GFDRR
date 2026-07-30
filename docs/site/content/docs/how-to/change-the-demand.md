---
title: "Change the demand"
weight: 1
---

# How to change the demand a run faces

Task: run the same scenario with scaled demand, or replay a different month.

A run's demand table is built from one raw trip CSV. Two multipliers control
what the run faces — both are flags of `app/runner.py`:

- `--sizing-scale` — the demand level the state (initial inventory and dock
  capacities) is sized to survive with no loss;
- `--demand-scale` — the demand level the run actually faces.

Equal values give a clean run with zero losses. A run scale above the sizing
scale makes the limits take effect: stockout and dock-full events appear
(sized run: [Notations.md §11](../reference/notations.md#11-run-kinds)). Every scaled
count is rounded to whole bikes by one shared rule, `scale_demand` in
`gbp/consumers/simulator/mechanics.py`.

## Scale the demand

Run double demand against a state sized for the historical level:

```bash
python app/runner.py --run-name demand_x2 --demand-scale 2.0 --periods 50
```

Expected result: the runner prints the saved folder (`data/runs/demand_x2/`),
the invariant count, and the totals. The extra demand hits the limits, so
`lost_demand` is no longer zero; when docks fill up, `redirected` and
`lost_dock_full` follow.

To let the state absorb the doubled demand, size it for the same level:

```bash
python app/runner.py --run-name demand_x2_sized \
    --demand-scale 2.0 --sizing-scale 2.0 --periods 50
```

Expected result: a clean run at double volume — `Invariant violations: 0` and
zero loss totals.

## Replay another month

Download the month's trip CSV and point the runner at it:

```bash
python -m domains.citybike.loaders.download --months 202602
python app/runner.py --run-name feb_replay \
    --trips-path data/raw/202602-citibike-tripdata_1.csv
```

Expected result: `data/runs/feb_replay/meta.json` names the CSV under
`inputs`, and its `t0` is that month's earliest trip hour.

Either way, `meta.json` records `demand_scale_factor` and
`sizing_scale_factor`, and the saved run appears in the web interface
(`streamlit run app/main.py`).
