---
title: "Installation"
weight: 1
---

# Installation

This page takes you from a clean clone to a working install. Step 1 is
enough for the [quickstart](quickstart.md), which needs no data. Step 2
downloads one month of real trips — the [UI tutorial](ui.md) and the full
runs need it. Docker and OSRM are optional and covered by links at the end.

Run every command from the repository root.

## Step 1. Install the package

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/).

```bash
uv venv
uv pip install -e ".[ui]"          # simulator + web interface
```

Other extras, added the same way:

```bash
uv pip install -e ".[dev,ui,api]"     # plus lint, type check, tests, and the run-artifact API
uv pip install -e ".[dev,ui,api,ml]"  # plus the demand-forecast pipeline
```

Install with `-e` (editable): the package then runs from the source tree,
so code edits take effect without a reinstall. Exact dependency versions
are pinned in `uv.lock`.

Check the install:

```bash
python -c "from gbp.consumers.simulator import Environment; print('ok')"
```

The command prints `ok`. If it fails with `ModuleNotFoundError`, the
virtual environment is not active — activate it
(`source .venv/bin/activate`) or call `.venv/bin/python` directly.

## Step 2. Download real trip data (optional)

The full runs replay a month of Citi Bike trips. Those CSVs are not stored
in git: Citi Bike publishes monthly zip files at
`https://s3.amazonaws.com/tripdata`. One month unpacks to 0.3–0.9 GB of
CSVs; a year of months is about 9 GB on disk.

Download one month into `data/raw/` (the data folders are Notations.md §15):

```bash
python -m gbp.loaders.download --months 202601
```

The command streams the month's zip files, unpacks them, and ends with the
line `Done: <count> new files in .../data/raw` — a month unpacks to a few
CSVs. The CSVs are named
`202601-citibike-tripdata_1.csv`, `_2`, and so on — the default input of
`app/runner.py` is the `_1` file of month 202601.

## Docker (optional)

`docker compose up --build` starts the web interface and the OSRM routing
server together. The command and the data-folder layout are in the root
[README.md](https://github.com/vlzm/GFDRR), section "Run with Docker".

## OSRM (optional)

By default the simulator measures distances with the haversine formula and
needs no server. To use road-network distances instead, set up the OSRM
server: [how-to/set-up-osrm.md](../how-to/set-up-osrm.md).

## Where to go next

- [quickstart.md](quickstart.md) — run the engine on a synthetic scenario
  and read your first flow journal; needs only step 1.
- [ui.md](ui.md) — create a first saved run on the downloaded month and
  browse it in the web interface; needs steps 1 and 2.
