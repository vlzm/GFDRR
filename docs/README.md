# Documentation — start here

A framework for problems on flow graphs — networks where commodities move
between facilities; the first and so far only scenario is the Citi Bike
bike-sharing system in New York City. This page is the map of `docs/`:

- To run it, go to [Getting Started](#getting-started).
- To understand it, go to [Key Components](#key-components).
- To change it, go to [How-to](#how-to).
- To find the exact contract, go to [Reference](#reference).

## Getting Started

Tutorials: follow the steps and compare with the expected output.

- [quickstart.md](getting-started/quickstart.md) — from a clean clone to a
  first flow journal in a few minutes, on a synthetic scenario with no data
  download; ends with the real-data variant (where the trip CSVs come from,
  which command downloads a month).

## Key Components

Explanations: how each part works and why it is built this way — no
step-by-step instructions. Read the first two pages first; every other page
assumes them.

- [concepts.md](concepts.md) — the five concepts: period, demand, station
  inventory, flow journal, run artifact.
- [architecture.md](explanation/architecture.md) — the system as diagrams
  at three zoom levels, ending with the full module map.
- [data-model.md](key-components/data-model.md) — the loaders: from the raw
  trip CSV to the input tables the simulator reads.
- [flow-journal.md](key-components/flow-journal.md) — the journal library
  (`gbp/model/`): the append-only table that holds the full history of a run.
- [worked-examples.md](key-components/worked-examples.md) — the smallest
  real journal tables, one scenario each; every table is re-run by a test.
- [simulation-engine.md](key-components/simulation-engine.md) — how one run
  works: the period loop, its phases, the invariants.
- [rebalancing.md](key-components/rebalancing.md) — how the simulator moves
  bikes by truck at night.
- [ml-toolkit.md](key-components/ml-toolkit.md) — the demand-forecast
  pipeline: training, backtesting, champion promotion, monitoring.
- [visualization.md](key-components/visualization.md) — from a finished run
  to the screen: the runner, the run artifact, the Streamlit app.

## Scenarios

The first scenario is the Citi Bike system in New York City. Its canonical
form is two runs, one notebook each:

- [test_pipeline.ipynb](../notebooks/test_pipeline.ipynb) — the base replay
  of one month of history.
- [forecast_pipeline.ipynb](../notebooks/forecast_pipeline.ipynb) — the same
  run on forecast demand.

## How-to

Recipes, one task per page: the steps, real commands, the expected result.
A new recipe is added when the same task comes up twice.

- [change-the-demand.md](how-to/change-the-demand.md) — scale the demand a
  run faces, or replay another month.
- [run-on-a-forecast.md](how-to/run-on-a-forecast.md) — run the simulator
  on a saved forecast instead of history.
- [add-a-table-to-the-run-artifact.md](how-to/add-a-table-to-the-run-artifact.md)
  — precompute a new table into every saved run.
- [debug-an-invariant-violation.md](how-to/debug-an-invariant-violation.md)
  — find which invariant (I1–I5) broke and which journal rows to look at.
- [set-up-osrm.md](how-to/set-up-osrm.md) — set up the optional
  road-network routing server.

## Reference

Exact contracts, for lookup — not a reading route:

- [Notations.md](../Notations.md) (repository root) — the project
  dictionary: every column, status, and table name; one concept, one word.
- [api.md](reference/api.md) — the HTTP contract of the run-artifact API.
- [reference/README.md](reference/README.md) — the section index and the
  command table.

## Decisions

Design decisions that span several modules, one short record each;
single-module decisions stay in each page's "Why It Is Built This Way" section.

- [journal-as-source-of-truth.md](decisions/journal-as-source-of-truth.md)
  — the run is kept as an append-only journal, not one editable state table.
- [sizing-run.md](decisions/sizing-run.md) — the initial inventory and dock
  capacities are measured by a sizing run, not loaded from a file.
- [forecast-replaces-only-demand.md](decisions/forecast-replaces-only-demand.md)
  — a forecast run replaces only the demand table of the base replay.

## The rest of docs/

`plans/` holds the plans of the current work; `reports/` holds saved
evaluation and review reports; `method/` holds the author's personal notes
on how to work — background reading, not part of the route above. The
documents are English; this page has a Russian companion,
[README_ru.md](README_ru.md), kept in sync.
