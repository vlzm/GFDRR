# Citi Bike Simulation Platform

A bike-sharing simulator built on the Citi Bike domain: the `gbp/` package runs
the simulation, the `app/` folder is a Streamlit web interface that shows saved
runs. The canonical scenario lives in `notebooks/test_pipeline.ipynb`.

## Install

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/).

```bash
uv venv
uv pip install -e ".[ui]"          # simulator + web interface
uv pip install -e ".[dev,ui]"      # plus lint, type check, tests
```

Exact dependency versions are pinned in `uv.lock` (refresh with `uv lock`
after changing `pyproject.toml`).

## Run

```bash
streamlit run app/main.py                                        # web interface
python app/runner.py --run-name demo --demand-scale 1.5 --periods 50   # one scenario from the terminal
```

A finished run is saved as a folder under `<data dir>/runs/<run_name>/`;
the web interface lists every saved run.

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
  raw/    # source trip CSVs (e.g. 202602-citibike-tripdata_1.csv)
  osrm/   # road graph files for the OSRM server (see docs/osrm_setup.md)
  runs/   # saved run artifacts, one folder per run
```

## Development

```bash
ruff check gbp/ tests/ app/       # lint
ruff format gbp/ tests/ app/      # format
mypy gbp/                         # type check
pytest                            # tests
```
