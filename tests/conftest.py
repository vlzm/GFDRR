"""Shared pytest fixtures, auto-discovered for every test in this folder.

pytest loads ``conftest.py`` automatically, so a test uses a fixture just by
naming it as an argument -- no import needed.
"""

import pytest

from tests import scenarios


@pytest.fixture(scope="session")
def run_scenario():
    """Run a named scenario once, then reuse the result across the whole session.

    Returns a callable ``name -> (resolved, journal, state)``. Each scenario is
    built and simulated at most once and cached by name, so the universal sweep
    and the per-scenario tests share a single run instead of repeating it. The
    results are read-only -- tests only assert on them -- so sharing is safe.
    """
    cache: dict[str, tuple] = {}

    def _run(name: str) -> tuple:
        if name not in cache:
            resolved = scenarios.ALL_SCENARIOS[name]()
            journal, state = scenarios.run(resolved)
            cache[name] = (resolved, journal, state)
        return cache[name]

    return _run
