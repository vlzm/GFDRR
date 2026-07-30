"""The one place the app chooses its backend: local files, or HTTP when API_URL is set."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TYPE_CHECKING

import api_client
import pandas as pd
import streamlit as st

from gbp import artifacts
from gbp.ml import artifact

if TYPE_CHECKING:
    from gbp.consumers.run import RunRequest

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
    from gbp.consumers import run  # heavy import (pulls the simulator); only a local run pays it

    return run.build_graph_data(trips_path)


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
        return artifacts.table_path(run_name, table).stat().st_mtime

    def load_meta(self, run_name: str) -> artifacts.RunMeta:
        """Read one saved run's ``meta.json``."""
        return artifacts.load_run_meta(run_name)

    def meta_cache_key(self, run_name: str) -> float:
        """Return the file's mtime, for the same reason as table_cache_key."""
        return artifacts.meta_path(run_name).stat().st_mtime

    def list_forecasts(self) -> list[str]:
        """Names of the saved forecasts (``data/ml/forecasts/``) a run can use."""
        return artifact.list_forecasts()

    def default_trips_path(self) -> str:
        """Return the trips CSV a local run reads by default; the user may point elsewhere."""
        from gbp.consumers import run

        return run.DEFAULT_TRIPS_PATH

    def save_location(self, requested_name: str) -> str:
        """Where the run will land: its artifact folder, under the free name."""
        return str(artifacts.run_dir(artifacts.next_free_run_name(requested_name)))

    def run_and_wait(
        self, request: RunRequest, trips_path: str | None, on_progress: Callable[[str], None]
    ) -> str:
        """Run one scenario in this process; return the final run name."""
        from gbp.consumers import run

        on_progress("Loading data (a few minutes the first time; cached afterwards)…")
        graph_data = _graph_data_cached(trips_path)
        final_name = artifacts.next_free_run_name(request.run_name)
        run.run_scenario(
            graph_data,
            request.model_copy(update={"run_name": final_name}),
            on_progress=on_progress,
        )
        return final_name


class ApiBackend:
    """Runs on the server; HTTP reads and starts (docs/site/content/docs/reference/api.md)."""

    def list_runs(self) -> list[str]:
        """Names of every saved run on the server."""
        return [meta.run_name for meta in api_client.list_runs()]

    def load_table(self, run_name: str, table: str) -> pd.DataFrame:
        """Fetch one saved parquet table over HTTP."""
        return api_client.load_table(run_name, table)

    def table_cache_key(self, run_name: str, table: str) -> float:
        """Return a constant cache key (a served artifact never changes)."""
        return 0.0

    def load_meta(self, run_name: str) -> artifacts.RunMeta:
        """Fetch one saved run's ``meta.json`` over HTTP."""
        return api_client.load_meta(run_name)

    def meta_cache_key(self, run_name: str) -> float:
        """Return a constant, for the same reason as table_cache_key."""
        return 0.0

    def list_forecasts(self) -> None:
        """Return ``None``: the page cannot list the server's forecasts."""
        return None

    def default_trips_path(self) -> None:
        """Return ``None``: which data the server runs on is a server setting."""
        return None

    def save_location(self, requested_name: str) -> str:
        """Say where the run will land; the server resolves the final name itself."""
        return f"{requested_name} on the server"

    def run_and_wait(
        self, request: RunRequest, trips_path: str | None, on_progress: Callable[[str], None]
    ) -> str:
        """Queue one run on the server and poll it to the end (raises RunFailed on failure)."""
        del trips_path
        answer = api_client.start_run(request.model_dump())
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
    """Return the chosen backend: HTTP when ``API_URL`` is set, else the disk."""
    return ApiBackend() if api_client.api_url() else DiskBackend()
