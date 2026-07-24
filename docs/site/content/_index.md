---
title: Home
type: docs
---

# A simulator for operational decisions on a flow graph

A **flow graph** is a network where some commodity moves between facilities.
This project builds one platform for problems on such graphs, and implements
one domain end to end: the **Citi Bike** bike-sharing system in New York —
about 2,300 stations and almost five million trips a month.

The operator faces a constant question: *is a decision worth the money before
we spend it?* You cannot A/B-test a truck fleet on a real city. So the answer
here is a **simulator that replays the city's real history exactly** — then you
change one input, run it again, and read the operational cost of the change
straight from the output.

![The Streamlit run browser: one saved run, page by page.](images/ui_overview.png)

## What is technically interesting

- **An append-only flow journal is the single source of truth.** Every run is
  one growing table of events; every number a page shows is computed from it.
- **Exact historical replay.** The base run reproduces a real month of trips
  move for move — not a resample — so any change is measured against ground
  truth.
- **Two-level evaluation of a demand forecast.** A forecast is judged twice:
  by its error against actual demand, and by what the simulator does when it
  runs on that forecast with the physics held fixed.
- **A full ML toolkit around it:** training, rolling-origin backtesting,
  champion promotion through an MLflow registry, and drift monitoring.

## By the numbers

From the January 2026 evaluation (`docs/reports/model_evaluation_202601.md`):
a one-week run of **168 hourly periods**, **424,191 trips**, **66,895 bikes**
across **128,537 docks** — five runs (one reference, four forecasts), each
passing every run invariant.

## Where to go next

- **[See the scenario]({{< relref "/docs/scenario" >}})** — the Citi Bike
  problem, the data, and what the system does.
- **[Understand it]({{< relref "/docs/architecture" >}})** — how a run works:
  the journal, the period loop, rebalancing, the forecast toolkit.
- **[See why it is built this way]({{< relref "/docs/decisions" >}})** — the
  design decisions behind the architecture.
- **[Run it yourself]({{< relref "/docs/getting-started" >}})** — install,
  first run, and the web interface.
