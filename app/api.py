"""HTTP service over the run artifacts (docs/explanation/api.md).

The API is a reader and a saver of run artifacts (Notations.md §12): it
serves the saved files as they are, and it starts runs through the same
``runner.run_scenario`` the Run scenario page calls. It computes nothing
``artifacts.build_run_tables`` can precompute, and it adds nothing to
``gbp/``.

Serve with::

    uvicorn api:app --app-dir app

Server settings (environment variables): ``DATA_DIR`` (the data folder),
``TRIPS_PATH`` and ``ROUTING_MODE`` (the one dataset runs are built from),
``API_KEY`` (the shared access key; unset = the check is off, for local
development).
"""

from __future__ import annotations

import os
import queue
import threading
from typing import Literal

import artifacts
import pydantic
import runner
from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Response

from gbp.loaders.dataloader_graph import ResolvedModelData

PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"


class RunRequest(pydantic.BaseModel):
    """The body of ``POST /runs``: the keyword parameters of ``runner.run_scenario``.

    ``run_name`` is restricted to plain file-name characters, so it always
    names a folder inside the runs root. There is no trips-path field on
    purpose: which data the server runs on is a server setting
    (``TRIPS_PATH``), not a request field.
    """

    run_name: str = pydantic.Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    demand_scale_factor: float
    sizing_scale_factor: float = 1.0
    number_of_periods: int = runner.DEFAULT_NUMBER_OF_PERIODS
    demand_source: Literal["history", "forecast"] = "history"
    #: A saved forecast on the server's disk (``data/ml/forecasts/``);
    #: required when ``demand_source="forecast"``.
    forecast_name: str | None = None
    rebalancing: bool = False
    truck_homes: list[str] | None = None
    truck_capacity_bikes: int = runner.DEFAULT_TRUCK_CAPACITY_BIKES


class RunState(pydantic.BaseModel):
    """The state of one started run: the ``GET /runs/{run_name}/status`` payload.

    ``progress`` collects the ``on_progress`` messages ``run_scenario`` emits
    -- the same lines the Run scenario page prints locally. ``error`` carries
    the exception text when the run failed.
    """

    run_name: str
    status: Literal["queued", "running", "done", "failed"]
    progress: list[str] = pydantic.Field(default_factory=list)
    error: str | None = None


#: Every run started by this process, by final run name. Lives in memory
#: only: after a restart the status endpoint falls back to the disk.
_run_states: dict[str, RunState] = {}
_states_lock = threading.Lock()

#: Queued runs, in POST order. One single-thread worker pulls them off one at
#: a time: one run uses the machine fully, and ten users share one server.
_jobs: queue.Queue[tuple[RunRequest, RunState]] = queue.Queue()
_worker: threading.Thread | None = None

#: The server's one resolved dataset (only the worker thread touches it).
_graph_data: ResolvedModelData | None = None


def _server_graph_data() -> ResolvedModelData:
    """Return the server's one ``ResolvedModelData``, built on first use and kept.

    ``build_graph_data`` takes minutes, so the first run pays it once and
    every later run reuses the tables -- the same reuse the Run scenario page
    gets from ``st.cache_resource``. Only the worker thread calls this, so no
    lock is needed.
    """
    global _graph_data
    if _graph_data is None:
        _graph_data = runner.build_graph_data(
            trips_path=os.environ.get("TRIPS_PATH", runner.DEFAULT_TRIPS_PATH),
            routing_mode=os.environ.get("ROUTING_MODE", "haversine"),
        )
    return _graph_data


def _worker_loop() -> None:
    """Run queued scenarios one at a time, recording progress on the run state."""
    while True:
        request, state = _jobs.get()
        state.status = "running"
        try:
            if _graph_data is None:
                state.progress.append(
                    "Loading the server dataset (a few minutes; kept for later runs)"
                )
            runner.run_scenario(
                _server_graph_data(),
                run_name=state.run_name,
                demand_scale_factor=request.demand_scale_factor,
                sizing_scale_factor=request.sizing_scale_factor,
                number_of_periods=request.number_of_periods,
                demand_source=request.demand_source,
                forecast_name=request.forecast_name,
                rebalancing=request.rebalancing,
                truck_homes=request.truck_homes,
                truck_capacity_bikes=request.truck_capacity_bikes,
                on_progress=state.progress.append,
            )
            state.status = "done"
        except Exception as exc:  # the error goes to the status payload
            state.status = "failed"
            state.error = f"{type(exc).__name__}: {exc}"


def _ensure_worker() -> None:
    """Start the single-thread worker on the first ``POST /runs``."""
    global _worker
    with _states_lock:
        if _worker is None:
            _worker = threading.Thread(target=_worker_loop, name="run-worker", daemon=True)
            _worker.start()


def _free_run_name(base: str) -> str:
    """Pick the final name of a started run: ``base``, or ``base_version_{i}``.

    ``next_free_run_name`` only sees saved folders. A queued or running run
    has no folder yet, so its name must be skipped here as well -- otherwise
    two quick POSTs with the same name would write into one folder. Call
    with ``_states_lock`` held.
    """
    name = artifacts.next_free_run_name(base)
    if name not in _run_states:
        return name
    taken = set(artifacts.list_runs()) | set(_run_states)
    i = 2
    while f"{base}_version_{i}" in taken:
        i += 1
    return f"{base}_version_{i}"


def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    """Check the shared key on every endpoint except ``/health``.

    The server reads ``API_KEY`` from the environment and compares it with
    the ``X-API-Key`` header. With ``API_KEY`` unset (local development) the
    check is off.
    """
    expected = os.environ.get("API_KEY")
    if expected and x_api_key != expected:
        raise HTTPException(status_code=401, detail="missing or wrong X-API-Key header")


app = FastAPI(title="Citi Bike run artifacts", description="See docs/explanation/api.md.")
protected = APIRouter(dependencies=[Depends(require_api_key)])


@app.get("/health")
def health() -> dict[str, str]:
    """Liveness answer for the platform health probe; needs no key."""
    return {"status": "ok"}


@protected.get("/runs")
def get_runs() -> list[artifacts.RunMeta]:
    """Return the ``meta.json`` of every complete run artifact.

    A folder counts as a run only when its ``meta.json`` exists -- the same
    rule ``list_runs`` uses -- so a run being written is not listed.
    """
    return [artifacts.load_run_meta(name) for name in artifacts.list_runs()]


@protected.get("/runs/{run_name}")
def get_run_meta(run_name: str) -> artifacts.RunMeta:
    """One saved run's ``meta.json``."""
    if run_name not in artifacts.list_runs():
        raise HTTPException(status_code=404, detail=f"unknown run {run_name!r}")
    return artifacts.load_run_meta(run_name)


@protected.get("/runs/{run_name}/tables/{table}")
def get_run_table(run_name: str, table: str) -> Response:
    """One parquet table of a saved run, as the saved bytes.

    ``table`` is one of the ``RUN_TABLES`` stems. The client reads the body
    with ``pd.read_parquet(io.BytesIO(response.content))``.
    """
    if table not in artifacts.RUN_TABLES or run_name not in artifacts.list_runs():
        raise HTTPException(status_code=404, detail=f"unknown run or table {run_name!r}/{table!r}")
    path = artifacts.table_path(run_name, table)
    return Response(content=path.read_bytes(), media_type=PARQUET_MEDIA_TYPE)


@protected.post("/runs", status_code=202)
def start_run(request: RunRequest) -> dict[str, str]:
    """Queue one run; answer with the final run name to poll.

    Every started run goes through the free-name rule, so a saved artifact
    is never overwritten. The client polls ``GET /runs/{run_name}/status``
    with the returned name.
    """
    with _states_lock:
        name = _free_run_name(request.run_name)
        state = RunState(run_name=name, status="queued")
        _run_states[name] = state
    _ensure_worker()
    _jobs.put((request, state))
    return {"run_name": name, "status": "queued"}


@protected.get("/runs/{run_name}/status")
def get_run_status(run_name: str) -> RunState:
    """Return the state of a started run, from memory; from the disk after a restart.

    A restart loses the in-memory run states. The disk then answers: an
    existing ``meta.json`` means the run completed (``done``); anything else
    is unknown (``404``). A run that was executing during the restart has no
    ``meta.json``, so it is lost -- and never listed by ``GET /runs``.
    """
    state = _run_states.get(run_name)
    if state is not None:
        return state
    if run_name in artifacts.list_runs():
        return RunState(run_name=run_name, status="done")
    raise HTTPException(status_code=404, detail=f"unknown run {run_name!r}")


app.include_router(protected)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, port=8000)
