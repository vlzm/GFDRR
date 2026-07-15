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
development). Endpoint details are in the docstrings of `app/api.py` — this
page gives the contract and the design.

## Why The API Exists

On one machine the UI reads the parquet files of `data/runs/<run_name>/`
directly. On a shared server the saved runs live on the server's disk, so
every client — the Streamlit app, a script, a second service — needs one way
to reach them. The API is that way: the network form of the same artifact
contract the UI reads locally
([Notations.md §12](../../Notations.md#12-run-artifacts-the-files-the-ui-reads)).
It serves saved files and starts runs by calling the same
`runner.run_scenario` the Run scenario page calls. It computes nothing
`artifacts.build_run_tables` can precompute, and it adds nothing to `gbp/`.

## The Service Map

Two processes on the server, one shared `data/` folder:

```text
runner worker (inside the API process)
        │ writes data/runs/<run_name>/
        ▼
data/runs/  ◄── reads ──  FastAPI (app/api.py)  ◄── HTTP ──  Streamlit (app/main.py)
                                                ◄── HTTP ──  any other client
```

The API process is the only writer of `data/runs/` on the server. The worker
is a single thread that runs one scenario at a time — one run uses the
machine fully — and builds the server's one `ResolvedModelData` from the
configured trip CSV on its first run, keeping it for every later run. A
`POST /runs` only queues; run states live in process memory, and after a
restart the disk answers instead (a `meta.json` means `done`).

## Endpoints

| Method and path | Returns |
|---|---|
| `GET /health` | `{"status": "ok"}` — the platform health probe; needs no key |
| `GET /runs` | `list[RunMeta]` — the `meta.json` of every saved run |
| `GET /runs/{run_name}` | one `RunMeta`; `404` for an unknown run |
| `GET /runs/{run_name}/tables/{table}` | the saved parquet bytes (`flows`, `panel`, `arcs`, `flow_totals`, `facilities`) |
| `POST /runs` | `202` + the final `run_name` to poll |
| `GET /runs/{run_name}/status` | `queued` / `running` / `done` / `failed`, plus `progress` lines and `error` |

The `POST /runs` body mirrors the keyword parameters of
`runner.run_scenario` (`demand_scale_factor`, `number_of_periods`,
`demand_source`, `forecast_name`, `rebalancing`, `truck_homes`, ...). The
server resolves the final run name itself through the free-name rule, so two
quick `POST`s with the same name never write into one folder. Two inputs are
server settings, not request fields: the trips path (`TRIPS_PATH`) and the
forecast list (`data/ml/forecasts/` on the server's disk). There is no
version prefix and no delete endpoint: one client and one server ship from
one repository, and the UI has no delete.

Access control is one shared key: a FastAPI dependency checks the
`X-API-Key` header on every endpoint except `/health`. `app/api_client.py`
is a small module of plain functions that never imports fastapi or
streamlit; the two sides meet only at the HTTP contract, and `RunMeta`
(`app/artifacts.py`) stays the one definition of `meta.json` on both.

## Artifacts Are Immutable

On the server, the API is the only writer, and every write goes through the
free-name rule. So once a run's `meta.json` exists, that folder never
changes. This rule replaces cache invalidation: the local loader caches a
table with the file's modification time in the key; over HTTP the client
caches by `(run_name, table)` alone, with no expiry, because an existing
artifact cannot change.

## Non-Goals

No database (the runs folder is the storage), no computation endpoints
(pages slice tables after loading), no JSON form of the tables (parquet is
the one wire format; `RunMeta` is the one JSON payload), no parallel runs,
no per-user runs.

## Why It Is Built This Way

### The Artifact Contract Is The API Contract

The folder layout of Notations.md §12 already is a complete, schema-checked
interface. Inventing a second contract for HTTP would mean two contracts to
keep in sync; serving the first one over HTTP means zero.

### The API Sits Beside The Loader, Not Inside `gbp/`

The simulation layer does not know runs are served over a network, just as
it does not know Streamlit exists. Both consumers meet at
`app/artifacts.py`.

### Parquet Over The Wire

`flows.parquet` can hold millions of rows. Re-encoding it as JSON would cost
time and memory on both sides and create a second schema to keep in sync.
The tables are already schema-checked at save time; the cheapest correct
move is to send the saved bytes.

### One Writer Makes Artifacts Immutable

Immutability is what lets ten clients cache tables with no invalidation
protocol, and lets the status endpoint recover from a restart by looking at
the disk.
