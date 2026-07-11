"""The one place the app chooses its backend: local files, or HTTP when API_URL is set.

Every path from the app to a run goes through the object :func:`current`
returns — reading saved runs (the ``ui_shared`` loaders call it) and starting
new ones (the Run scenario page calls it). Pages never import ``api_client``
and never check ``API_URL`` themselves, so the two backends cannot drift
page by page: the choice is made here, once.

``DiskBackend`` reads ``data/runs/`` and executes runs in this process.
``ApiBackend`` talks to the run-artifact API (docs/explanation/api.md)
through ``api_client``. The two classes answer the same calls, so a caller
never asks which one it holds — except the Run page, which asks
:meth:`list_forecasts` and :meth:`default_trips_path` and reads ``None`` as
"the server owns this, the page cannot pick".
"""

from __future__ import annotations

import time
from collections.abc import Callable

import api_client
import artifacts
import pandas as pd
import streamlit as st

#: Seconds between two polls of a run executing on the server.
_POLL_SECONDS = 2


class RunFailed(Exception):
    """A started run ended in the ``failed`` state; ``str(exc)`` is the error text."""

    def __init__(self, run_name: str, error: str) -> None:
        super().__init__(error)
        self.run_name = run_name


@st.cache_resource(show_spinner=False)
def _graph_data_cached(trips_path: str):
    """Load the raw data once per source; later runs reuse the tables."""
    import runner  # heavy import (pulls the simulator); only a local run pays it

    return runner.build_graph_data(trips_path)


class DiskBackend:
    """Runs live under the local data folder; a started run executes in this process."""

    def list_runs(self) -> list[str]:
        """Names of every saved run on the disk."""
        return artifacts.list_runs()

    def load_table(self, run_name: str, table: str) -> pd.DataFrame:
        """Read one saved parquet table."""
        return artifacts.load_run_table(run_name, table)

    def table_cache_key(self, run_name: str, table: str) -> float:
        """Return the file's mtime: a local file can be rewritten, so it keys the cache."""
        return (artifacts.run_dir(run_name) / f"{table}.parquet").stat().st_mtime

    def load_meta(self, run_name: str) -> artifacts.RunMeta:
        """Read one saved run's ``meta.json``."""
        return artifacts.load_run_meta(run_name)

    def meta_cache_key(self, run_name: str) -> float:
        """Return the file's mtime, for the same reason as :meth:`table_cache_key`."""
        return (artifacts.run_dir(run_name) / "meta.json").stat().st_mtime

    def list_forecasts(self) -> list[str]:
        """Names of the saved forecasts (``data/ml/forecasts/``) a run can use."""
        from gbp.ml import forecast  # heavy import; only the Run page pays it

        return forecast.list_forecasts()

    def default_trips_path(self) -> str:
        """Return the trips CSV a local run reads by default; the user may point elsewhere."""
        import runner

        return runner.DEFAULT_TRIPS_PATH

    def save_location(self, requested_name: str) -> str:
        """Where the run will land: its artifact folder, under the free name."""
        return str(artifacts.run_dir(artifacts.next_free_run_name(requested_name)))

    def run_and_wait(
        self, request: dict, trips_path: str | None, on_progress: Callable[[str], None]
    ) -> str:
        """Run one scenario in this process; return the final run name.

        ``request`` holds the keyword parameters of ``runner.run_scenario`` —
        the same fields ``POST /runs`` takes. The requested name goes through
        the free-name rule here, exactly as the server does it, so a saved
        run is never overwritten.
        """
        import runner

        on_progress("Loading data (a few minutes the first time; cached afterwards)…")
        graph_data = _graph_data_cached(trips_path)
        final_name = artifacts.next_free_run_name(request["run_name"])
        runner.run_scenario(
            graph_data, **{**request, "run_name": final_name}, on_progress=on_progress
        )
        return final_name


class ApiBackend:
    """Runs live on the server; reads and starts go over HTTP (docs/explanation/api.md)."""

    def list_runs(self) -> list[str]:
        """Names of every saved run on the server."""
        return [meta.run_name for meta in api_client.list_runs()]

    def load_table(self, run_name: str, table: str) -> pd.DataFrame:
        """Fetch one saved parquet table over HTTP."""
        return api_client.load_table(run_name, table)

    def table_cache_key(self, run_name: str, table: str) -> float:
        """Return a constant cache key.

        A served artifact never changes (the API is the only writer on the
        server and never overwrites a saved run — docs/explanation/api.md),
        so ``(run_name, table)`` alone identifies the content.
        """
        return 0.0

    def load_meta(self, run_name: str) -> artifacts.RunMeta:
        """Fetch one saved run's ``meta.json`` over HTTP."""
        return api_client.load_meta(run_name)

    def meta_cache_key(self, run_name: str) -> float:
        """Return a constant, for the same reason as :meth:`table_cache_key`."""
        return 0.0

    def list_forecasts(self) -> None:
        """Return ``None``: the page cannot list the server's forecasts.

        The forecasts live on the server's disk; the server rejects an
        unknown name when the run starts.
        """
        return None

    def default_trips_path(self) -> None:
        """Return ``None``: which data the server runs on is a server setting.

        ``TRIPS_PATH`` on the server names the dataset; the page cannot pick.
        """
        return None

    def save_location(self, requested_name: str) -> str:
        """Say where the run will land; the server resolves the final name itself."""
        return f"{requested_name} on the server"

    def run_and_wait(
        self, request: dict, trips_path: str | None, on_progress: Callable[[str], None]
    ) -> str:
        """Queue one run on the server (``POST /runs``) and poll it to the end.

        The status endpoint carries the same progress lines ``on_progress``
        prints locally; each new line is passed on. ``trips_path`` is ignored:
        the server runs on its own dataset. Raises :class:`RunFailed` with
        the server's error text when the run fails.
        """
        del trips_path
        answer = api_client.start_run(request)
        final_name = answer["run_name"]
        on_progress(f"Queued on the server as {final_name}.")
        shown = 0
        while True:
            state = api_client.run_status(final_name)
            for line in state["progress"][shown:]:
                on_progress(line)
            shown = len(state["progress"])
            if state["status"] == "done":
                return final_name
            if state["status"] == "failed":
                raise RunFailed(final_name, state["error"])
            time.sleep(_POLL_SECONDS)


def current() -> DiskBackend | ApiBackend:
    """Return the chosen backend: HTTP when ``API_URL`` is set, else the disk.

    This is the one place that choice is made.
    """
    return ApiBackend() if api_client.api_url() else DiskBackend()
