# Documentation — start here

The project is a simulator of a bike-sharing system, built on real Citi Bike
data. The input is a month of trips; the simulator replays that demand period
by period, and every change is written as one row of the flow journal — an
append-only table that alone holds the full history of a run. The current
phase is demand forecasting: a model predicts future demand, and the simulator
runs on that forecast next to the base replay on history. The canonical
scenario is therefore two runs — the base replay in
`notebooks/test_pipeline.ipynb` and the forecast run in
`notebooks/forecast_pipeline.ipynb`; everything in the codebase must serve one
of them.

Pick the door that matches what you came for.

## Run it

[quickstart.md](getting-started/quickstart.md) takes a clean clone to its first flow journal in
a few minutes, on a synthetic scenario — no data download. The same page ends
with the real-data variant: where the trip CSVs come from, how much they
weigh, which command downloads a month.

## Understand it

Three short reads, from concrete to general:

1. [concepts.md](concepts.md) — the five concepts every other page assumes:
   period, demand, station inventory, flow journal, run artifact.
2. [worked-examples.md](key-components/worked-examples.md), scenarios 1–3 — the smallest
   real journal tables: a stockout, a trip that docks in the same period, a
   trip that docks a period later. Every table there is re-run by a test.
3. [architecture.md](explanation/architecture.md) — the system as diagrams at
   three zoom levels, ending with the full module map.

## Change it

The common changes have a recipe in [how-to/](how-to/) — one page each: the
task, the steps with real commands and files, the expected result.

- [change-the-demand.md](how-to/change-the-demand.md) — scale the demand a
  run faces, or replay another month.
- [run-on-a-forecast.md](how-to/run-on-a-forecast.md) — run the simulator on
  a saved forecast instead of history.
- [add-a-table-to-the-run-artifact.md](how-to/add-a-table-to-the-run-artifact.md)
  — precompute a new table into every saved run.
- [debug-an-invariant-violation.md](how-to/debug-an-invariant-violation.md)
  — find which invariant (I1–I5) broke and which journal rows to look at.

A new recipe is added when the same task comes up twice. For a change
without one, find the part you are changing on the module map in
[architecture.md](explanation/architecture.md), then open its document:

| Part | Document |
|---|---|
| raw trip CSV → simulator inputs | [data-model.md](key-components/data-model.md) |
| the simulation loop | [simulation-engine.md](key-components/simulation-engine.md) |
| truck rebalancing | [rebalancing.md](key-components/rebalancing.md) |
| the journal library (`gbp/model/flows.py`) | [flow-journal.md](key-components/flow-journal.md) |
| run artifacts and the web interface | [visualization.md](key-components/visualization.md) |
| the run-artifact API | [api.md](reference/api.md) |
| demand forecasting | [ml-toolkit.md](key-components/ml-toolkit.md) |

Each document ends with a "Why It Is Built This Way" section: the
load-bearing decisions, each with the alternative that was rejected and the
reason. Decisions that span several modules live as short records in
[decisions/](decisions/).

## Find the exact contract

[Notations.md](../Notations.md) (repository root) is the project dictionary:
every column, status, and table name — one concept, one word. Open it by
section when you need a contract; it is a reference, not a reading route.
The HTTP contract is in [api.md](reference/api.md); the command list is in
the root [README.md](../README.md).

## The rest of docs/

- `key-components/` — the per-module documents from the table above.
- `how-to/` — the task recipes from the "Change it" door above, plus
  [set-up-osrm.md](how-to/set-up-osrm.md): the optional road-network
  routing server.
- `decisions/` — short records of design decisions that cannot be derived
  from the code, one record per decision.
- `plans/` — the plans of the current work.
- `reports/` — saved evaluation and review reports.
- `method/` — the author's personal notes on how to work.
  [comprehension_levels.md](method/comprehension_levels.md) describes a way to
  read a codebase in five levels of understanding — background reading, not
  part of the route above.

## Languages

The canonical language of the documents is English. A Russian companion
(`*_ru.md`) exists only for the files the author rereads regularly: this page
([README_ru.md](README_ru.md)),
[comprehension_levels_ru.md](method/comprehension_levels_ru.md), and
[working-method.ru.md](method/working-method.ru.md). Each pair is kept in
sync.
