"""HTTP client of the run-artifact API (docs/reference/api.md): plain functions.

The Streamlit loader (``ui_shared.py``) calls these when ``API_URL`` is set.
This module never imports fastapi or streamlit: the API side and the client
side meet only at the HTTP contract, which is the artifact contract of
Notations.md §12 served over the network. ``RunMeta`` (``app/artifacts.py``)
stays the one definition of ``meta.json``; the tables travel as the saved
parquet bytes.
"""

from __future__ import annotations

import io
import os

import artifacts
import pandas as pd
import requests

#: Seconds before an API call fails; a table download can carry millions of rows.
_TIMEOUT_SECONDS = 120


def api_url() -> str | None:
    """Return the base URL of the run-artifact API; ``None`` means read local files."""
    url = os.environ.get("API_URL", "").strip()
    return url.rstrip("/") or None


def _base() -> str:
    """Return the base URL, required: calling the API without ``API_URL`` is a bug."""
    url = api_url()
    if url is None:
        raise RuntimeError("API_URL is not set; the API client has nowhere to call")
    return url


def _headers() -> dict[str, str]:
    """Build the shared-key header when ``API_KEY`` is set (docs/reference/api.md)."""
    key = os.environ.get("API_KEY")
    return {"X-API-Key": key} if key else {}


def _get(path: str) -> requests.Response:
    """One GET against the API; raises on connection errors and 4xx/5xx answers."""
    response = requests.get(f"{_base()}{path}", headers=_headers(), timeout=_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response


def list_runs() -> list[artifacts.RunMeta]:
    """Fetch the ``meta.json`` of every saved run on the server (``GET /runs``)."""
    return [artifacts.RunMeta.model_validate(item) for item in _get("/runs").json()]


def load_meta(run_name: str) -> artifacts.RunMeta:
    """One saved run's ``meta.json`` (``GET /runs/{run_name}``)."""
    return artifacts.RunMeta.model_validate(_get(f"/runs/{run_name}").json())


def load_table(run_name: str, table: str) -> pd.DataFrame:
    """One parquet table of a saved run (``GET /runs/{run_name}/tables/{table}``)."""
    return pd.read_parquet(io.BytesIO(_get(f"/runs/{run_name}/tables/{table}").content))


def start_run(request: dict) -> dict:
    """Queue one run (``POST /runs``); returns the final run name and ``"queued"``."""
    response = requests.post(
        f"{_base()}/runs", json=request, headers=_headers(), timeout=_TIMEOUT_SECONDS
    )
    response.raise_for_status()
    return response.json()


def run_status(run_name: str) -> dict:
    """Fetch the state of a started run (``GET /runs/{run_name}/status``)."""
    return _get(f"/runs/{run_name}/status").json()
