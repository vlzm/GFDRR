"""Deterministic random-number generation for simulation phases.

Phases that sample (e.g., trip destination sampling) must obtain their RNG
from ``make_rng`` rather than from the global ``numpy.random`` state.  The
generator returned by ``make_rng`` depends only on the triple
``(global_seed, phase_name, period_index)`` — so the RNG for a given phase in
a given period is identical regardless of execution order.
"""

from __future__ import annotations

import hashlib

import numpy as np


def make_rng(
    global_seed: int | None,
    phase_name: str,
    period_index: int,
) -> np.random.Generator:
    """Build a deterministic ``numpy.random.Generator`` for one (phase, period).

    The seed is derived from ``(global_seed, phase_name, period_index)`` via
    BLAKE2b, giving a uniform 64-bit value per call.  Same inputs → same RNG
    → byte-identical samples on repeated runs.

    Parameters
    ----------
    global_seed
        Seed from ``EnvironmentConfig.seed``.  When ``None``,
        a non-deterministic generator is returned (avoid in production
        simulations — used only when reproducibility is intentionally off).
    phase_name
        ``Phase.name`` of the calling phase.
    period_index
        ``PeriodRow.period_index`` of the current period.

    Returns
    -------
    numpy.random.Generator
        A NumPy ``Generator`` seeded deterministically from the three inputs.
    """
    if global_seed is None:
        return np.random.default_rng()
    key = f"{global_seed}|{phase_name}|{period_index}".encode()
    digest = hashlib.blake2b(key, digest_size=8).digest()
    seed_int = int.from_bytes(digest, "big", signed=False)
    return np.random.default_rng(seed_int)
