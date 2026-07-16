"""Tests of the run-artifact API (``app/api.py``, described in ``docs/reference/api.md``).

The API runs under FastAPI's ``TestClient`` against synthetic saved runs (the
offline scenarios of ``tests/scenarios.py``) in a tmp data folder. The heavy
pipeline is replaced by a fake ``run_scenario`` that saves a real artifact,
so the POST tests exercise the queue, the states, and the free-name rule
without a trip CSV.
"""

import io
import pathlib
import sys
import time

import pandas as pd
import pytest

pytest.importorskip("fastapi")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "app"))

import api  # noqa: E402  (needs the app folder on sys.path)

from gbp import artifacts  # noqa: E402
from gbp.consumers import run  # noqa: E402
from gbp.consumers.run import RunRequest  # noqa: E402
from tests import scenarios  # noqa: E402
from tests.test_app_artifacts import _save_run  # noqa: E402


@pytest.fixture
def runs_root(tmp_path, monkeypatch):
    """Two synthetic saved runs under a tmp data folder, with clean API state."""
    root = tmp_path / "runs"
    for name, builder in [("base", scenarios.overflow), ("scaled", scenarios.stockout)]:
        resolved = builder()
        journal, _ = scenarios.run(resolved)
        _save_run(name, resolved, journal, root)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.delenv("API_KEY", raising=False)
    api._run_states.clear()
    api._graph_data = None
    return root


@pytest.fixture
def client(runs_root):
    from fastapi.testclient import TestClient

    return TestClient(api.app)


@pytest.fixture
def fake_runner(monkeypatch, runs_root):
    """Replace the heavy pipeline with a fast fake that saves a real artifact."""
    monkeypatch.setattr(run, "build_graph_data", lambda **kwargs: "graph-data")

    def fake_run_scenario(graph_data, request, on_progress=None):
        if on_progress is not None:
            on_progress("Sizing the state, running the simulation, checking the invariants I1-I5")
        resolved = scenarios.canonical()
        journal, _ = scenarios.run(resolved)
        _save_run(request.run_name, resolved, journal, runs_root)
        return runs_root / request.run_name

    monkeypatch.setattr(run, "run_scenario", fake_run_scenario)


def _wait_until_finished(client, run_name, timeout=30.0):
    """Poll the status endpoint until the run ends; the worker is a thread."""
    deadline = time.monotonic() + timeout
    state = None
    while time.monotonic() < deadline:
        state = client.get(f"/runs/{run_name}/status").json()
        if state["status"] in ("done", "failed"):
            return state
        time.sleep(0.05)
    raise AssertionError(f"run {run_name} did not finish in time: {state}")


# ---------------------------------------------------------------------------
# Reading: health, run list, meta, tables
# ---------------------------------------------------------------------------
def test_health_answers_without_a_key(client, monkeypatch):
    monkeypatch.setenv("API_KEY", "secret")
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_runs_lists_the_meta_of_every_saved_run(client):
    response = client.get("/runs")
    assert response.status_code == 200
    metas = response.json()
    assert [meta["run_name"] for meta in metas] == ["base", "scaled"]
    # The payload is the meta.json contract: RunMeta must validate it back.
    for meta in metas:
        artifacts.RunMeta.model_validate(meta)


def test_single_run_meta_and_unknown_run(client):
    response = client.get("/runs/base")
    assert response.status_code == 200
    assert response.json()["run_name"] == "base"
    assert client.get("/runs/no_such_run").status_code == 404


def test_table_endpoint_serves_the_saved_parquet_bytes(client, runs_root):
    response = client.get("/runs/base/tables/panel")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith(api.PARQUET_MEDIA_TYPE)
    served = pd.read_parquet(io.BytesIO(response.content))
    saved = artifacts.load_run_table("base", "panel", runs_root)
    pd.testing.assert_frame_equal(served, saved)


def test_unknown_table_or_run_is_404(client):
    assert client.get("/runs/base/tables/no_such_table").status_code == 404
    assert client.get("/runs/no_such_run/tables/panel").status_code == 404


def test_api_key_guards_every_endpoint_but_health(client, monkeypatch):
    monkeypatch.setenv("API_KEY", "secret")
    assert client.get("/runs").status_code == 401
    assert client.get("/runs", headers={"X-API-Key": "wrong"}).status_code == 401
    assert client.get("/runs", headers={"X-API-Key": "secret"}).status_code == 200


# ---------------------------------------------------------------------------
# Starting runs: queue, states, the free-name rule
# ---------------------------------------------------------------------------
def test_post_runs_executes_and_saves(client, fake_runner):
    response = client.post("/runs", json={"run_name": "api_run", "demand_scale_factor": 1.0})
    assert response.status_code == 202
    assert response.json() == {"run_name": "api_run", "status": "queued"}

    state = _wait_until_finished(client, "api_run")
    assert state["status"] == "done"
    assert state["error"] is None
    assert any("simulation" in line for line in state["progress"])
    assert "api_run" in [meta["run_name"] for meta in client.get("/runs").json()]


def test_post_runs_never_overwrites_a_saved_run(client, fake_runner):
    # "base" is already saved on disk: the POST must get a versioned name.
    response = client.post("/runs", json={"run_name": "base", "demand_scale_factor": 1.0})
    assert response.json()["run_name"] == "base_version_2"
    assert _wait_until_finished(client, "base_version_2")["status"] == "done"


def test_two_quick_posts_with_one_name_get_two_names(client, fake_runner):
    # A queued run has no folder yet; its name must be skipped all the same.
    first = client.post("/runs", json={"run_name": "twice", "demand_scale_factor": 1.0})
    second = client.post("/runs", json={"run_name": "twice", "demand_scale_factor": 1.0})
    names = {first.json()["run_name"], second.json()["run_name"]}
    assert names == {"twice", "twice_version_2"}
    for name in names:
        assert _wait_until_finished(client, name)["status"] == "done"


def test_failed_run_reports_the_error(client, fake_runner, monkeypatch):
    def boom(*args, **kwargs):
        raise ValueError("boom")

    monkeypatch.setattr(run, "run_scenario", boom)
    response = client.post("/runs", json={"run_name": "bad", "demand_scale_factor": 1.0})
    state = _wait_until_finished(client, response.json()["run_name"])
    assert state["status"] == "failed"
    assert "boom" in state["error"]
    # A failed run wrote no meta.json, so the run list never shows it.
    assert "bad" not in [meta["run_name"] for meta in client.get("/runs").json()]


def test_run_name_with_path_separators_is_rejected(client, fake_runner):
    response = client.post("/runs", json={"run_name": "../escape", "demand_scale_factor": 1.0})
    assert response.status_code == 422


def test_status_falls_back_to_the_disk(client):
    # No in-memory record (as after a restart): meta.json on disk answers done.
    response = client.get("/runs/base/status")
    assert response.status_code == 200
    assert response.json() == {"run_name": "base", "status": "done", "progress": [], "error": None}
    assert client.get("/runs/no_such_run/status").status_code == 404


# ---------------------------------------------------------------------------
# The client side: the API_URL switch
# ---------------------------------------------------------------------------
def test_api_url_reads_the_environment(monkeypatch):
    import api_client

    monkeypatch.delenv("API_URL", raising=False)
    assert api_client.api_url() is None
    monkeypatch.setenv("API_URL", "http://localhost:8000/")
    assert api_client.api_url() == "http://localhost:8000"


def test_backend_choice_follows_api_url(monkeypatch):
    # backend.current() is the one place the disk-or-API choice is made.
    import backend

    monkeypatch.delenv("API_URL", raising=False)
    assert isinstance(backend.current(), backend.DiskBackend)
    monkeypatch.setenv("API_URL", "http://localhost:8000")
    assert isinstance(backend.current(), backend.ApiBackend)


def test_table_cache_key_is_a_constant_over_http(monkeypatch):
    # Served artifacts are immutable (docs/reference/api.md): the HTTP cache key must
    # not touch the disk and must not vary.
    import ui_shared

    monkeypatch.setenv("API_URL", "http://localhost:8000")
    assert ui_shared.table_cache_key("any_run", "panel") == 0.0


def test_api_backend_forwards_progress_and_returns_the_final_name(monkeypatch):
    # run_and_wait polls the status endpoint and passes each new progress
    # line on exactly once; the server's (possibly versioned) name comes back.
    import api_client
    import backend

    monkeypatch.setattr(backend.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(
        api_client, "start_run", lambda request: {"run_name": "x_version_2", "status": "queued"}
    )
    states = iter(
        [
            {"status": "running", "progress": ["step 1"], "error": None},
            {"status": "done", "progress": ["step 1", "step 2"], "error": None},
        ]
    )
    monkeypatch.setattr(api_client, "run_status", lambda name: next(states))

    lines: list[str] = []
    name = backend.ApiBackend().run_and_wait(
        RunRequest(run_name="x"), None, on_progress=lines.append
    )
    assert name == "x_version_2"
    assert lines == ["Queued on the server as x_version_2.", "step 1", "step 2"]


def test_api_backend_raises_run_failed_with_the_server_error(monkeypatch):
    import api_client
    import backend

    monkeypatch.setattr(
        api_client, "start_run", lambda request: {"run_name": "bad", "status": "queued"}
    )
    monkeypatch.setattr(
        api_client,
        "run_status",
        lambda name: {"status": "failed", "progress": [], "error": "ValueError: boom"},
    )
    with pytest.raises(backend.RunFailed) as caught:
        backend.ApiBackend().run_and_wait(
            RunRequest(run_name="bad"), None, on_progress=lambda line: None
        )
    assert caught.value.run_name == "bad"
    assert "boom" in str(caught.value)
