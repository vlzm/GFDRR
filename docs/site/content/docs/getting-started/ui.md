---
title: "First look at the web interface"
weight: 3
---

# First look at the web interface

This page creates a first saved run and opens it in the web interface. The
interface is a reader: it lists the runs saved under `data/runs/` — each one
a run artifact (Notations.md §12) — and draws their tables. A clean clone
has no saved runs, so the first step creates one.

Prerequisites: [installation.md](installation.md) step 1 with the `[ui]`
extra, and step 2 (one downloaded month of trips). Run everything from the
repository root.

## Step 1. Create a saved run

```bash
python app/runner.py --run-name demo --demand-scale 1.5 --periods 50
```

The runner reads `data/raw/202601-citibike-tripdata_1.csv` (`--trips-path`
picks another file), replays 50 periods with the demand scaled 1.5×, and
saves the result under `data/runs/demo/`. Loading and preparing take
minutes, not seconds. The command prints progress lines and ends with:

```text
Saved .../data/runs/demo
Invariant violations: 0
Totals: ...
```

`python app/runner.py --help` lists the other options: rebalancing trucks,
running on a saved forecast, OSRM routing.

## Step 2. Start the interface

```bash
streamlit run app/main.py
```

Streamlit prints a local URL — usually `http://localhost:8501` — and opens
it in the browser. The first page is Overview & compare, with the run
`demo` selected:

![The Overview & compare page of the web interface](images/ui_overview.png)

The sidebar has two pickers, shared by every page: Scenario A is the run
you look at, Scenario B is an optional second run drawn next to it for
comparison. With a single saved run, leave B empty.

## Step 3. Walk the pages

The page list is the left navigation. Every page reads the saved run; the
only page that computes anything heavy is "Run scenario".

- Overview & compare — whole-run totals of scenario A, the side-by-side
  comparison with B, and the table of all saved runs.
- Run scenario — set the parameters and start a new run from the browser;
  it saves a new artifact, same as `app/runner.py`.
- Station map — the stations on a map, colored by one metric in one chosen
  period; hovering a station shows every metric for A and B.
- Trips map — arcs of the flows riding in the chosen period, colored by
  outcome.
- Truck trips — the bikes trucks moved during overnight rebalancing, one
  arc per move; empty unless the run used `--rebalancing`.
- Costs — cost charts: whole-run total, per period, per commodity, per
  facility.
- Distance & duration — the same four cuts for trip distance and duration.
- Single facility — one facility: its inventory over periods, any metric
  per period, the raw panel rows.
- Model monitoring — the forecast-model metric history and drift reports
  saved by `python -m gbp.ml.monitoring`; empty until that command has run.
- Download data — the raw output tables of the selected runs as CSV files.

## Where to go next

- [How the interface is built](../architecture/visualization.md) — the
  runner, the run artifact, and the app, and why the app only reads.
- [change-the-demand.md](../how-to/change-the-demand.md) — create runs that
  differ (another scale, another month) and compare them as A and B.
