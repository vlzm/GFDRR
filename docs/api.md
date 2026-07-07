# API design: serving run artifacts over HTTP

## Why an API

Today the UI and the data live on one machine. Streamlit reads the parquet
files of `data/runs/<run_name>/` directly, so nothing sits between the pages
and the disk, and nothing needs to.

On a shared server this changes. The users' browsers talk to Streamlit, but a
future non-Streamlit frontend, a script, or a second service would each need
its own way to reach the saved runs. The API gives them one: a small HTTP
service that serves the run artifacts. It is the network form of the same
contract the UI already reads — Notations.md §12.

The API stays a **reader and a saver of run artifacts**. It serves saved
files, and it starts runs by calling the same `runner.run_scenario` the Run
scenario page calls today. It never computes anything
`artifacts.build_run_tables` can precompute, and it adds nothing to `gbp/`.

## What does not change

| Piece | Stays as is because |
|---|---|
| `gbp/` | The API is a consumer, like the UI. No new abstractions. |
| `app/artifacts.py` | Still the one place that builds, saves, and loads artifacts. The API calls `list_runs`, `load_run_meta`, `next_free_run_name`; it serves the parquet files as they are. |
| `app/runner.py` | Still owns `build_graph_data` and `run_scenario`. The API's worker calls them unchanged. |
| The artifact contract | `meta.json` + five parquet tables per run (Notations.md §12). The API exposes this contract; it does not define a second one. |
| `views/*` pages | Pages keep reading through the `ui_shared` typed accessors. Only the loader behind those accessors changes (see below). |

## The service map

Two processes on the server, one shared `data/` folder:

```
runner worker (inside the API process)
        │ writes data/runs/<run_name>/
        ▼
data/runs/  ◄── reads ──  FastAPI (app/api.py)  ◄── HTTP ──  Streamlit (app/main.py)
                                                  ◄── HTTP ──  any other client
```

- The API process is the **only writer** of `data/runs/` on the server.
- Streamlit becomes a client: its loader fetches tables over HTTP instead of
  opening files. Locally, without a server, it keeps reading the disk
  directly (see "Changes in the Streamlit app").
- `DATA_DIR` already moves the data folder (`artifacts.data_dir`), so the
  server can mount shared storage without code changes.

The server holds one dataset: `build_graph_data` takes minutes, so the API
builds one `ResolvedModelData` from its configured trip CSV (env vars
`TRIPS_PATH`, `ROUTING_MODE`) on the first `POST /runs` and keeps it for every
later run — the same reuse the Run scenario page gets from
`st.cache_resource` today. Clients cannot pass a trips path; which data the
server runs on is a server setting, not a request field.

## Endpoints

| Method and path | Returns | Mirrors |
|---|---|---|
| `GET /health` | `{"status": "ok"}` | — (for the Azure health probe) |
| `GET /runs` | `list[RunMeta]` — the `meta.json` of every saved run | `list_runs` + `load_run_meta` |
| `GET /runs/{run_name}` | one `RunMeta` | `load_run_meta` |
| `GET /runs/{run_name}/tables/{table}` | the parquet file bytes | `load_run_table` |
| `POST /runs` | `202` + the final `run_name` | the Run scenario page |
| `GET /runs/{run_name}/status` | the state of a started run | the page's progress box |

There is no version prefix and no delete endpoint: one client and one server
ship from one repository, and the UI has no delete today.

### `GET /runs`

The list the Overview & compare page needs: the `meta.json` of every complete
artifact, as JSON. A folder counts as a run only when its `meta.json` exists —
the same rule `list_runs` uses, so a run being written is not listed.

`RunMeta` is already a pydantic model (`app/artifacts.py`), so FastAPI returns
it directly. The `meta.json` contract stays defined in that one place.

### `GET /runs/{run_name}/tables/{table}`

`table` is one of the `RUN_TABLES` stems: `flows`, `panel`, `arcs`,
`flow_totals`, `facilities`. The response body is the parquet file as saved,
media type `application/vnd.apache.parquet`; the client reads it with
`pd.read_parquet(io.BytesIO(response.content))`.

Parquet bytes, not JSON, on purpose: `flows.parquet` can hold millions of
rows. Re-encoding it as JSON would cost time and memory on both sides and
would create a second schema to keep in sync. Serving the file keeps the API
thin and keeps the pandera schemas (`RUN_TABLE_SCHEMAS`) the only table
contract.

Unknown `run_name` or unknown `table` → `404`.

### `POST /runs`

Starts one run. The body mirrors the keyword parameters of
`runner.run_scenario`:

```json
{
  "run_name": "demand_x2",
  "demand_scale_factor": 2.0,
  "sizing_scale_factor": 1.0,
  "number_of_periods": 50,
  "rebalancing": false,
  "truck_homes": null,
  "truck_capacity_bikes": 20
}
```

The server passes `run_name` through `next_free_run_name` and answers `202`
with the final name:

```json
{"run_name": "demand_x2_version_2", "status": "queued"}
```

The client polls `GET /runs/{run_name}/status` with that name. Because every
started run goes through `next_free_run_name`, a saved artifact on the server
is **never overwritten** — see "Artifacts are immutable" below.

A second `POST` while a run is executing is accepted and queued; the worker
runs one scenario at a time (a single-thread worker), because one run uses
the machine fully and ten users share one server.

### `GET /runs/{run_name}/status`

The state of a started run:

```json
{
  "run_name": "demand_x2_version_2",
  "status": "running",
  "progress": ["Sizing the state, running the simulation, checking the invariants I1-I5"],
  "error": null
}
```

`status` is one of `queued`, `running`, `done`, `failed`. `progress` collects
the `on_progress` messages `run_scenario` emits — the same lines the Run
scenario page prints today. `error` carries the exception text when `failed`.

The run states live in the API process memory, not on disk. If the process
restarts, that record is gone; the endpoint then falls back to the disk: an
existing `meta.json` answers `done`, anything else `404`. A run that was
executing during the restart is lost — its folder has no `meta.json`, so
`GET /runs` never lists the half-written result (the write-`meta.json`-last
rule of `save_run`).

## Artifacts are immutable

On the server, the API is the only writer, and every write goes through
`next_free_run_name`. So once a run's `meta.json` exists, that folder never
changes.

This rule replaces the mtime cache key. The local loader caches a table with
the file's modification time in the key; over HTTP the client caches by
`(run_name, table)` alone, with no expiry, because an existing artifact
cannot change. No cache invalidation protocol is needed.

## Changes in the Streamlit app

The loader in `ui_shared.py` is the one front door to a saved run, and that
is the payoff here: the pages, pickers, KPI row, and charts do not change.
Two files change:

- `ui_shared.py` — `_load_table`, the meta loader, and the run list get two
  backends behind the same functions: the local file reads (today's code)
  when `API_URL` is unset, HTTP calls to the API when it is set. The typed
  accessors (`load_panel`, `load_arcs`, `load_flow_totals`,
  `load_facilities`, `load_meta`) keep their signatures.
- `views/run_scenario.py` — with `API_URL` set, the page sends `POST /runs`
  and polls `GET /runs/{run_name}/status`, showing the `progress` lines in
  the status box instead of receiving them through `on_progress`. Without
  `API_URL` it keeps calling `runner.run_scenario` directly.

The HTTP client is a small module of plain functions (`app/api_client.py`),
so the API-side code and the client-side code never import each other's
frameworks: `api.py` never imports streamlit, `api_client.py` never imports
fastapi.

Local development stays a one-process command: `streamlit run app/main.py`
with no `API_URL` behaves exactly as today. The server runs both processes
and sets `API_URL=http://localhost:8000` for Streamlit.

## Access control

Up to ten known users, one shared server. The first version uses one shared
key: the server reads `API_KEY` from the environment and a FastAPI dependency
checks the `X-API-Key` header on every endpoint except `/health`; with
`API_KEY` unset (local development) the check is off. Per-user accounts,
if ever needed, would come from the platform in front (Azure App Service
authentication), not from this codebase.

## Non-goals

- **No database.** The runs folder is the storage; `meta.json` presence marks
  a complete run. The API adds no state of its own beyond the in-memory run
  states.
- **No computation in the API.** No filtering, aggregation, or slicing
  endpoints. Pages slice tables after loading, exactly as they do now; every
  precomputable value already lives in the artifact.
- **No JSON form of the tables.** Parquet is the one wire format for tables;
  `RunMeta` is the one JSON payload.
- **No parallel runs.** One worker, one run at a time.
- **No per-user runs.** Every user sees every run.

## Why it is built this way

**The artifact contract is the API contract.** The folder layout of
Notations.md §12 already is a complete, versioned-by-`code_version`,
validated-by-schemas interface. Inventing a second contract for HTTP would
mean two contracts to keep in sync; serving the first one over HTTP means
zero.

**The API sits beside the loader, not inside `gbp/`.** The simulation layer
does not know runs are served over a network, just as it does not know
Streamlit exists. Both consumers meet at `app/artifacts.py`.

**Parquet over the wire.** The tables are large, columnar, and already
schema-checked at save time. The cheapest correct move is to send the saved
bytes.

**One writer makes artifacts immutable.** Immutability is what lets ten
clients cache tables with no invalidation protocol and lets the status
endpoint recover from a restart by looking at the disk.
