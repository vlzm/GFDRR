# gbp — Generalized Framework for Graph-Based Problems

A framework for problems on flow graphs — networks where commodities move
between facilities; the first and so far only scenario is the Citi Bike
bike-sharing system in New York City.

## What this is

Many systems are flow graphs once you name the parts:

- logistics: goods move between warehouses and customers, carried by trucks;
- an electrical circuit: current moves between the nodes, along the wires;
- the internet: packets move between network nodes, along the links.

This project models such a system with four entities and one event log, and
builds tools on top of them: a simulator, a rebalancer, a demand forecast,
and a web interface for reading the results.

## The data model

Four entities describe a flow graph. The exact contracts live in
[Notations.md](Notations.md) — the project's dictionary; the section numbers
below point into it.

| Entity | Question it answers | In Citi Bike | In logistics |
|---|---|---|---|
| `commodity` (§8) | what moves | a bike (`classic_bike` / `electric_bike`) | goods |
| `edge` (§4b) | along what — a pair of facilities with a distance and a travel time | the route between two stations | a road between a warehouse and a customer |
| `resource` (§5b) | what carries it | a rebalancing truck | a truck |
| `facility` (§4) | where a flow starts, where it ends, where commodities are stored | a station or a depot | a warehouse or a customer |

The entities carry numbers and limits: an edge has a distance and a travel
time (§13), a flow accrues cost from a money rate over its riding time
(§6.1), and the domain adds its own constraints — dock capacity at a station
(§2), truck capacity (§14).

## Events

Everything that happens on the graph is an event: a commodity departs from a
facility, arrives at one, is redirected when it does not fit, or is lost. A
commodity moves on its own (a user rides a bike) or carried by a resource (a
truck moves bikes at night). The whole history of one run is one table — the
flow journal (§0): one row per event, four event types (`departed`,
`arrived`, `redirected`, `lost`). The tables the tools read — demand, the OD
matrix, arrivals — are computed from the journal, not stored separately (§9).

## What you can do

- Load raw flow data into the canonical tables: the loaders turn a month of
  trip CSVs into the simulator's input contract — the period grid,
  facilities, initial inventory, and the historical journal.
- Simulate: replay the historical trips through the engine period by period
  under the domain constraints — a full dock redirects an arriving bike, an
  empty station loses its demand.
- Rebalance: add truck phases that move bikes at night toward the inventory
  each station needs for the morning.
- Forecast demand: train models on past months, promote a champion, run the
  simulator on its forecast next to the base replay, and compare the two runs.

## Status

The first scenario is the Citi Bike system in New York City. Implemented
today: the replay simulator, truck rebalancing, the demand-forecast pipeline
(training, backtesting, champion promotion, monitoring), the web interface,
and the run-artifact API. The canonical scenario is two runs: the base
replay of history in `notebooks/test_pipeline.ipynb` and the run on forecast
demand in `notebooks/forecast_pipeline.ipynb`. The scenario page:
[docs/scenarios/citibike.md](docs/scenarios/citibike.md).

## Install

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/).

```bash
uv venv
uv pip install -e ".[ui]"          # simulator + web interface
uv pip install -e ".[dev,ui,api]"  # plus lint, type check, tests, and the run-artifact API
```

Activate the environment before running anything below
(`source .venv/bin/activate`), or call `.venv/bin/python` directly.
Exact dependency versions are pinned in `uv.lock` (refresh with `uv lock`
after changing `pyproject.toml`).

## Quick Start

The engine runs on a synthetic scenario without downloading any data. From
the repository root:

```bash
python - <<'PY'
from gbp.logging import configure_logging
from tests.scenarios import overflow, run

configure_logging()
journal, state = run(overflow())  # six bikes aim at a station with two free docks

bounced_id = journal.loc[journal["event_type"] == "redirected", "flow_id"].iloc[0]
cols = ["flow_id", "event_type", "reason", "source_id", "planned_target_id", "period_id"]
print(journal[journal["flow_id"] == bounced_id][cols].to_string(index=False))
print(state.state_inventory_df.to_string(index=False))
PY
```

After one log line, the script prints the journal of one redirected bike and
the final inventory:

```text
flow_id event_type    reason source_id planned_target_id  period_id
sim_0_2   departed      <NA>        s1                s3          0
sim_0_2 redirected dock_full        s1                s3          1
sim_0_2   departed      <NA>        s3                s2          1
sim_0_2    arrived      <NA>        s3                s2          1
facility_id commodity_category  quantity
         s1       classic_bike      47.0
         s2       classic_bike      51.0
         s3       classic_bike       2.0
```

Six bikes head for station `s3`, whose docks hold two. The journal shows one
of the four that did not fit: it departs `s1` toward `s3`, bounces off the
full dock (`redirected`, reason `dock_full`), and docks at the nearest free
station `s2`. The inventory confirms it: `s3` holds exactly its capacity
of 2. The step-by-step walkthrough of this run is
[docs/getting-started/quickstart.md](docs/getting-started/quickstart.md).

## Web interface

```bash
streamlit run app/main.py    # needs the [ui] extra
```

The interface lists the runs saved under `<data dir>/runs/`. To create one
from the terminal first:

```bash
python app/runner.py --run-name demo --demand-scale 1.5 --periods 50
```

![The Overview & compare page of the web interface](docs/assets/ui_overview.png)

The Overview & compare page shows the whole-run totals of a chosen run, an
optional side-by-side comparison with a second run, and the table of all
saved runs. Other pages draw the station map, individual trips, truck
routes, costs, and the model-monitoring report.

The same run artifacts are served over HTTP by the run-artifact API:
`uvicorn api:app --app-dir app` ([docs/reference/api.md](docs/reference/api.md)).

## Run with Docker

One command starts the web interface and the OSRM routing server together.
Requires Docker with the compose plugin and a `data/` folder next to the
repository files (see "Data layout" below).

```bash
docker compose up --build
```

The web interface is at http://localhost:8501. The `data/` folder is mounted
into the app container as `/data`, so finished runs land in `data/runs/` on
the host as usual. To run one scenario inside the container:

```bash
docker compose exec app python app/runner.py --run-name demo --demand-scale 1.5 --periods 50
```

## Configuration (environment variables)

Both variables are optional; without them the app runs against the local
repository layout.

| Variable | Meaning | Default |
|---|---|---|
| `DATA_DIR` | Root of the data folder: `raw/` (trip CSVs), `osrm/` (road graph), `runs/` (saved runs) | `data/` at the repository root |
| `OSRM_URL` | Base URL of the OSRM routing server (only used with `--routing osrm`) | `http://127.0.0.1:5000` |

## Data layout

```
<DATA_DIR>/
  raw/    # source trip CSVs (e.g. 202601-citibike-tripdata_1.csv)
  osrm/   # road graph files for the OSRM server (created by the setup in docs/how-to/set-up-osrm.md)
  runs/   # saved run artifacts, one folder per run
```

## Development

```bash
ruff check gbp/ tests/ app/       # lint
ruff format gbp/ tests/ app/      # format
mypy gbp/                         # type check
pytest                            # tests
```

## Documentation

The documentation starts at [docs/README.md](docs/README.md).
