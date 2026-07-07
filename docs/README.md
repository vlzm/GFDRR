# Documentation — start here

This page is the entry point to the project documentation. It says what the
project is, how to run it, what each folder of the repository does, and which
document to read next. The route follows the five levels of code
understanding from [comprehension_levels.md](comprehension_levels.md) — from
"I can run it" (level 1) to "I know every line" (level 5). Each level is
covered by exactly one artifact, and this page visits them in order.

## What this project is

The project is a simulator of a bike-sharing system, built on real Citi Bike
data. The input is a month of historical trips. The simulator replays that
demand period by period (a period is one step of the simulation clock,
[Notations.md §6](../Notations.md#6-time)): bikes leave stations, ride, and
dock at the destination — or, when the destination is full, get redirected to
a nearby station. Every change is written as one row of the flow journal
([Notations.md §0](../Notations.md#0-the-flow-event-schema-the-symbol-table)) —
a table that only ever receives new rows and alone holds the full history of
the run. Everything else the project shows (station inventories, maps, costs)
is computed from the journal.

Two parts sit on top of the simulator. The rebalancer moves bikes by truck at
night to the stations the morning demand would otherwise empty, so bikes are
there when the morning starts. The web interface (Streamlit) shows finished
runs: maps, inventory charts, trip tables.

Two files hold a special place. `notebooks/test_pipeline.ipynb` is the
canonical scenario — the source of truth: everything in the codebase must
serve it. [`Notations.md`](../Notations.md) is the project dictionary — one
concept, one word; every document here uses its vocabulary.

## How to run (level 1)

```bash
uv venv
uv pip install -e ".[dev,ui]"      # simulator + web interface + dev tools

python app/runner.py --run-name demo   # run one scenario with default settings
streamlit run app/main.py              # browse saved runs in the browser
pytest                                 # run the tests
```

`python app/runner.py --help` lists the scenario settings. A finished run is
saved as a folder under `data/runs/<run_name>/`; the web interface lists every
saved run. Docker, environment variables, and the layout of the data folder
are described in the [root README](../README.md). The optional road-network
routing server (OSRM) is set up by [osrm_setup.md](osrm_setup.md).

## The map of the repository (level 2)

One run flows through the repository like this:

raw trip CSVs → `RawModelData` → `ResolvedModelData` → simulator → flow
journal → run artifact → web interface

The same map as diagrams: [architecture.md](architecture.md) — the system and
the outside world (level 1), the big blocks (level 2), the full module map
with a depth table (level 3).

- **`gbp/`** — the library; nothing in it runs by itself. `gbp/loaders/` reads
  the raw trip CSVs into `RawModelData` and resolves them into
  `ResolvedModelData` — the input tables of the simulator (stations, demand,
  the OD matrix, the period grid). `gbp/consumers/simulator/` runs the
  simulation and produces the flow journal. `gbp/model/` holds the journal
  schema and the functions that read it. `gbp/routing.py` answers distance and
  travel-time questions for station pairs.
- **`app/`** — what you actually run. `app/runner.py` executes one scenario
  end to end and saves the result as a run artifact: a folder
  `data/runs/<run_name>/` with every table the interface needs, built by
  `app/artifacts.py`
  ([Notations.md §12](../Notations.md#12-run-artifacts-the-files-the-ui-reads)).
  `app/main.py` and `app/views/` are the Streamlit web interface — a pure
  reader of saved artifacts: it never simulates and never recomputes what the
  artifact builder has already computed.
- **`notebooks/`** — `test_pipeline.ipynb`, the canonical scenario: the whole
  pipeline in one notebook, from raw CSV to validated run. The other notebooks
  are working notebooks around specific parts of the code.
- **`data/`** — `raw/` (source trip CSVs), `osrm/` (road graph files for the
  routing server), `runs/` (saved run artifacts, one folder per run).
- **`tests/`** — `invariants.py` (the journal checks every run must pass),
  `scenarios.py` (builders of tiny synthetic runs), the test modules, and
  `test_docs_scenarios.py`, which re-runs every toy table of
  [scenarios.md](scenarios.md) so the documentation cannot silently drift from
  the code.

## Where to go next (levels 3–5)

Level 3 — every module and its contract, in coarse words. The module
diagrams and the module depth table are in
[architecture.md](architecture.md). Five documents
cover the run chain from the map above. [dataloader.md](dataloader.md): how
the raw trip CSV becomes `ResolvedModelData` — the entities, the historical
journal, the sized initial state. [simulator.md](simulator.md): what a period
is, what state the simulator carries, the phase loop, the mechanics, the
invariants. [rebalancing.md](rebalancing.md): how a truck plan is computed
and how the trucks execute it period by period.
[flow_journal.md](flow_journal.md): the journal library
(`gbp/model/flows.py`) shared by the loaders and the simulator — the event
schema, the builders, the read-models, the checks. [app.md](app.md): how a
finished run becomes a saved artifact and how the web interface draws it.

Level 4a — exact contracts. [`Notations.md`](../Notations.md) (repository
root) defines every column, status, and table name.
[scenarios.md](scenarios.md) shows the worked scenarios — each one a short
story, a sequence diagram, and a toy journal table reproduced by a test.

Level 4b — why it is built this way. Every level-3 document ends with a
"Why It Is Built This Way" section: the load-bearing decisions, each with
the alternative that was rejected and the reason. Start with
[simulator.md](simulator.md#why-it-is-built-this-way) and
[rebalancing.md](rebalancing.md#why-it-is-built-this-way).

Level 5 — line by line. Not a document: the code itself, entered through
`notebooks/test_pipeline.ipynb` and, for single modules, one-off walkthrough
notebooks in `notebooks/`.

[comprehension_levels.md](comprehension_levels.md) explains the route itself:
what each level means and when each is required.
[working-method.md](working-method.md) is the author's personal cheat sheet
about how to work; it is not documentation of the code.

## Languages

The canonical language of the documents is English. A Russian companion
(`*_ru.md`) exists only for the files the author rereads regularly: this page
([README_ru.md](README_ru.md)),
[comprehension_levels_ru.md](comprehension_levels_ru.md), and
[working-method.ru.md](working-method.ru.md). Each pair is kept in sync.
