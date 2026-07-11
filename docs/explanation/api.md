# The run-artifact API

This document describes `app/api.py` — the HTTP service over the run
artifacts — and `app/api_client.py`, the client the Streamlit app uses when
`API_URL` is set. The service starts with:

```bash
uvicorn api:app --app-dir app
```

Server settings are environment variables: `DATA_DIR` (the data folder),
`TRIPS_PATH` and `ROUTING_MODE` (the one dataset runs are built from), and
`API_KEY` (the shared access key; unset means the check is off, for local
development).

## Why The API Exists

On one machine the UI and the data live together: Streamlit reads the parquet
files of `data/runs/<run_name>/` directly, and nothing sits between the pages
and the disk. On a shared server the saved runs live on the server's disk, so
every client — the Streamlit app, a script, a second service — needs one way
to reach them. The API is that way: a small HTTP service that serves the run
artifacts. It is the network form of the same contract the UI reads locally —
[Notations.md §12](../../Notations.md#12-run-artifacts-the-files-the-ui-reads).

The API is a reader and a saver of run artifacts. It serves saved files, and
it starts runs by calling the same `runner.run_scenario` the Run scenario
page calls. It never computes anything `artifacts.build_run_tables` can
precompute, and it adds nothing to `gbp/`.

## The Pieces Around It

| Piece | Role |
|---|---|
| `gbp/` | Untouched. The API is a consumer, like the UI. |
| `app/artifacts.py` | The one place that builds, saves, and loads artifacts. The API calls `list_runs`, `load_run_meta`, `next_free_run_name`; the parquet files are served as saved. |
| `app/runner.py` | Owns `build_graph_data` and `run_scenario`. The API's worker calls them unchanged. |
| The artifact contract | `meta.json` + five parquet tables per run (Notations.md §12). The API exposes this contract; it does not define a second one. |
| `views/*` pages | Pages read through the `ui_shared` typed accessors and start runs through `backend.current()`; the disk-or-API choice lives in `app/backend.py` (see "The Two Backends"). |

## The Service Map

Two processes on the server, one shared `data/` folder:

```
runner worker (inside the API process)
        │ writes data/runs/<run_name>/
        ▼
data/runs/  ◄── reads ──  FastAPI (app/api.py)  ◄── HTTP ──  Streamlit (app/main.py)
                                                  ◄── HTTP ──  any other client
```

- The API process is the only writer of `data/runs/` on the server.
- Streamlit is a client: with `API_URL` set its loader fetches tables over
  HTTP instead of opening files. Locally, without `API_URL`, it reads the
  disk directly.
- `DATA_DIR` moves the data folder (`artifacts.data_dir`), so the server can
  mount shared storage without code changes.

The server holds one dataset. `build_graph_data` takes minutes, so the worker
thread builds one `ResolvedModelData` from the configured trip CSV (env vars
`TRIPS_PATH`, `ROUTING_MODE`) when it executes its first run, and keeps it
for every later run — the same reuse the Run scenario page gets from
`st.cache_resource`. A `POST /runs` request itself only queues the run: the
loading happens later, in the worker, and the first run's `progress` shows a
"Loading the server dataset" line. Clients cannot pass a trips path: which
data the server runs on is a server setting, not a request field.

## Endpoints

All endpoints live in `app/api.py`.

| Method and path | Returns |
|---|---|
| `GET /health` | `{"status": "ok"}` — the platform health probe; needs no key |
| `GET /runs` | `list[RunMeta]` — the `meta.json` of every saved run |
| `GET /runs/{run_name}` | one `RunMeta`; `404` for an unknown run |
| `GET /runs/{run_name}/tables/{table}` | the saved parquet bytes; `404` for an unknown run or table |
| `POST /runs` | `202` + the final `run_name` to poll |
| `GET /runs/{run_name}/status` | the state of a started run |

There is no version prefix and no delete endpoint: one client and one server
ship from one repository, and the UI has no delete.

### `GET /runs`

The list the Overview & compare page needs: the `meta.json` of every complete
artifact, as JSON. A folder counts as a run only when its `meta.json` exists —
the same rule `list_runs` uses, so a run being written is not listed.

`RunMeta` is a pydantic model (`app/artifacts.py`), so FastAPI returns it
directly. The `meta.json` contract stays defined in that one place.

### `GET /runs/{run_name}`

One saved run's `meta.json`, loaded with `load_run_meta`. An unknown
`run_name` answers `404`.

### `GET /runs/{run_name}/tables/{table}`

`table` is one of the `RUN_TABLES` stems: `flows`, `panel`, `arcs`,
`flow_totals`, `facilities`. The endpoint reads the saved file bytes directly
(`path.read_bytes()`) and returns them with media type
`application/vnd.apache.parquet`; the client reads the body with
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
  "demand_source": "history",
  "forecast_name": null,
  "rebalancing": false,
  "truck_homes": null,
  "truck_capacity_bikes": 20
}
```

`run_name` must match `^[A-Za-z0-9][A-Za-z0-9._-]*$` — plain file-name
characters, so it always names a folder inside the runs root. A name outside
the pattern is rejected with a validation error before anything is queued.

`demand_source` picks where the demand comes from: `"history"` (the default)
replays the historical demand; `"forecast"` starts a forecast run
([Notations.md §11](../../Notations.md#11-run-kinds)) — the run reads a saved
forecast demand table instead of history. `forecast_name` then names a
forecast artifact on the **server's** disk (`data/ml/forecasts/<forecast_name>/`,
[Notations.md §17](../../Notations.md#17-demand-forecasting-the-model-around-the-simulator)).
Like the trips path, the forecast list is a server matter: there is no
list-forecasts endpoint, the client sends a name, and an unknown name fails
the run (the `status` endpoint reports `failed` with the error text). The
run's `meta.json` records both fields.

The server resolves the final name itself and answers `202` with it:

```json
{"run_name": "demand_x2_version_2", "status": "queued"}
```

The name goes through `next_free_run_name`, and the names of queued or
running runs are skipped as well (they have no folder yet), so two quick
`POST`s with the same name never write into one folder. The client polls
`GET /runs/{run_name}/status` with the returned name. Because every started
run gets a free name, a saved artifact on the server is never overwritten —
see "Artifacts Are Immutable" below.

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
scenario page prints locally. `error` carries the exception text when
`failed`.

The run states live in the API process memory, not on disk. If the process
restarts, that record is gone; the endpoint then falls back to the disk: an
existing `meta.json` answers `done`, anything else `404`. A run that was
executing during the restart is lost — its folder has no `meta.json`, so
`GET /runs` never lists the half-written result (the write-`meta.json`-last
rule of `save_run`).

## Artifacts Are Immutable

On the server, the API is the only writer, and every write goes through the
free-name rule. So once a run's `meta.json` exists, that folder never
changes.

This rule replaces the mtime cache key. The local loader caches a table with
the file's modification time in the key; over HTTP the client caches by
`(run_name, table)` alone, with no expiry, because an existing artifact
cannot change. No cache invalidation protocol is needed.

## The Two Backends Of The Streamlit App

One module, `app/backend.py`, makes the disk-or-API choice: `backend.current()`
returns `ApiBackend` when `API_URL` is set and `DiskBackend` otherwise, and
everything the app does to runs goes through that object. Pages never import
`api_client` and never check `API_URL` themselves, and that is the payoff:
the pages, pickers, KPI row, and charts are the same in both backends.

- Reading. The `ui_shared.py` typed accessors (`load_panel`, `load_arcs`,
  `load_flow_totals`, `load_facilities`, `load_meta`) keep their signatures
  either way and call the backend underneath: local file reads on the disk,
  HTTP calls through `api_client` over the network. The cache key is the
  file's modification time locally and a constant over HTTP (see "Artifacts
  Are Immutable").
- Starting runs. `views/run_scenario.py` calls `backend.current().run_and_wait`.
  On the disk backend it runs `runner.run_scenario` in the Streamlit process
  and resolves the name with `next_free_run_name`; on the API backend it
  sends `POST /runs` and polls `GET /runs/{run_name}/status` (the server
  resolves the final run name). Both stream the same `progress` lines into
  the page's status box. Two inputs are server settings on the API backend:
  the trips path (`TRIPS_PATH`) and the forecast list (`data/ml/forecasts/`
  on the server's disk). The backend methods `default_trips_path()` and
  `list_forecasts()` return `None` there, so the page shows a plain
  forecast-name text field instead of a picker.

Local development stays a one-process command: `streamlit run app/main.py`
with no `API_URL` reads the disk directly. The server runs both processes and
sets `API_URL=http://localhost:8000` for Streamlit.

## The HTTP Client (`app/api_client.py`)

`app/api_client.py` is a small module of plain functions. It never imports
fastapi or streamlit — the API side and the client side meet only at the HTTP
contract. Every call raises on connection errors and on 4xx/5xx answers; the
timeout is 120 seconds, sized for a table download of millions of rows.

| Function | Endpoint | Returns |
|---|---|---|
| `api_url()` | — | The base URL from the `API_URL` env var; `None` means read local files. `backend.current()` reads it to choose the backend. |
| `list_runs()` | `GET /runs` | `list[RunMeta]` |
| `load_meta(run_name)` | `GET /runs/{run_name}` | one `RunMeta` |
| `load_table(run_name, table)` | `GET /runs/{run_name}/tables/{table}` | the table as a `pandas.DataFrame` |
| `start_run(request)` | `POST /runs` | `{"run_name": ..., "status": "queued"}` |
| `run_status(run_name)` | `GET /runs/{run_name}/status` | the run-state dict: `status`, `progress`, `error` |

`RunMeta` (`app/artifacts.py`) stays the one definition of `meta.json`: the
client validates every fetched meta back into that model. When `API_KEY` is
set, every call sends it in the `X-API-Key` header.

## Access Control

Up to ten known users, one shared server. One shared key: the server reads
`API_KEY` from the environment, and a FastAPI dependency checks the
`X-API-Key` header on every endpoint except `/health`. With `API_KEY` unset
(local development) the check is off. Per-user accounts, if ever needed,
would come from the platform in front (Azure App Service authentication),
not from this codebase.

## Non-Goals

- **No database.** The runs folder is the storage; `meta.json` presence marks
  a complete run. The API adds no state of its own beyond the in-memory run
  states.
- **No computation in the API.** No filtering, aggregation, or slicing
  endpoints. Pages slice tables after loading; every precomputable value
  already lives in the artifact.
- **No JSON form of the tables.** Parquet is the one wire format for tables;
  `RunMeta` is the one JSON payload.
- **No parallel runs.** One worker, one run at a time.
- **No per-user runs.** Every user sees every run.

## Why It Is Built This Way

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
