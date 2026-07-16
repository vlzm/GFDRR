"""HTTP service over the run artifacts."""

from __future__ import annotations

import os
import queue
import threading
from typing import Literal

import artifacts
import pydantic
import runner
from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Response
from runner import RunRequest

from gbp.loaders.dataloader_graph import ResolvedModelData

PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"


class RunState(pydantic.BaseModel):
    """The state of one started run: the ``GET /runs/{run_name}/status`` payload."""

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
    """Return the server's one ``ResolvedModelData``, built on first use and kept."""
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
            # The queued request still carries the requested name; the run must
            # use the free (possibly versioned) name resolved when it was
            # queued (``start_run``), so a saved artifact is never overwritten.
            runner.run_scenario(
                _server_graph_data(),
                request.model_copy(update={"run_name": state.run_name}),
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
    """Pick the final name of a started run: ``base``, or ``base_version_{i}``."""
    name = artifacts.next_free_run_name(base)
    if name not in _run_states:
        return name
    taken = set(artifacts.list_runs()) | set(_run_states)
    i = 2
    while f"{base}_version_{i}" in taken:
        i += 1
    return f"{base}_version_{i}"


def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    """Check the shared key on every endpoint except ``/health``."""
    expected = os.environ.get("API_KEY")
    if expected and x_api_key != expected:
        raise HTTPException(status_code=401, detail="missing or wrong X-API-Key header")


app = FastAPI(title="Citi Bike run artifacts", description="See docs/reference/api.md.")
protected = APIRouter(dependencies=[Depends(require_api_key)])


@app.get("/health")
def health() -> dict[str, str]:
    """Liveness answer for the platform health probe; needs no key."""
    return {"status": "ok"}


@protected.get("/runs")
def get_runs() -> list[artifacts.RunMeta]:
    """Return the ``meta.json`` of every complete run artifact."""
    return [artifacts.load_run_meta(name) for name in artifacts.list_runs()]


@protected.get("/runs/{run_name}")
def get_run_meta(run_name: str) -> artifacts.RunMeta:
    """One saved run's ``meta.json``."""
    if run_name not in artifacts.list_runs():
        raise HTTPException(status_code=404, detail=f"unknown run {run_name!r}")
    return artifacts.load_run_meta(run_name)


@protected.get("/runs/{run_name}/tables/{table}")
def get_run_table(run_name: str, table: str) -> Response:
    """One parquet table of a saved run, as the saved bytes."""
    if table not in artifacts.RUN_TABLES or run_name not in artifacts.list_runs():
        raise HTTPException(status_code=404, detail=f"unknown run or table {run_name!r}/{table!r}")
    path = artifacts.table_path(run_name, table)
    return Response(content=path.read_bytes(), media_type=PARQUET_MEDIA_TYPE)


@protected.post("/runs", status_code=202)
def start_run(request: RunRequest) -> dict[str, str]:
    """Queue one run; answer with the final run name to poll."""
    with _states_lock:
        name = _free_run_name(request.run_name)
        state = RunState(run_name=name, status="queued")
        _run_states[name] = state
    _ensure_worker()
    _jobs.put((request, state))
    return {"run_name": name, "status": "queued"}


@protected.get("/runs/{run_name}/status")
def get_run_status(run_name: str) -> RunState:
    """Return the state of a started run, from memory; from the disk after a restart."""
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
