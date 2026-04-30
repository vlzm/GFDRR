"""Tests for ``make_rng``: order-independent, deterministic RNG factory."""
# ruff: noqa: D102

from __future__ import annotations

import numpy as np

from gbp.consumers.simulator.random import make_rng


class TestMakeRng:
    """``make_rng`` derives a stable Generator from (seed, phase, period)."""

    def test_same_inputs_produce_same_samples(self) -> None:
        rng_a = make_rng(42, "TripSampling", 7)
        rng_b = make_rng(42, "TripSampling", 7)

        a = rng_a.integers(0, 1_000_000, size=10)
        b = rng_b.integers(0, 1_000_000, size=10)

        np.testing.assert_array_equal(a, b)

    def test_different_phase_diverges(self) -> None:
        rng_a = make_rng(42, "TripSampling", 7)
        rng_b = make_rng(42, "ODStructure", 7)

        a = rng_a.integers(0, 1_000_000, size=10)
        b = rng_b.integers(0, 1_000_000, size=10)

        assert not np.array_equal(a, b)

    def test_different_period_diverges(self) -> None:
        rng_a = make_rng(42, "TripSampling", 7)
        rng_b = make_rng(42, "TripSampling", 8)

        a = rng_a.integers(0, 1_000_000, size=10)
        b = rng_b.integers(0, 1_000_000, size=10)

        assert not np.array_equal(a, b)

    def test_different_seed_diverges(self) -> None:
        rng_a = make_rng(42, "TripSampling", 7)
        rng_b = make_rng(43, "TripSampling", 7)

        a = rng_a.integers(0, 1_000_000, size=10)
        b = rng_b.integers(0, 1_000_000, size=10)

        assert not np.array_equal(a, b)

    def test_none_seed_is_nondeterministic(self) -> None:
        """When seed is None, two calls produce independent generators.

        We sample many integers — the probability of collision should be
        negligibly small if the seeding is genuinely random.
        """
        rng_a = make_rng(None, "TripSampling", 7)
        rng_b = make_rng(None, "TripSampling", 7)

        a = rng_a.integers(0, 1_000_000_000, size=20)
        b = rng_b.integers(0, 1_000_000_000, size=20)

        assert not np.array_equal(a, b)

    def test_returns_numpy_generator(self) -> None:
        rng = make_rng(42, "TripSampling", 0)
        assert isinstance(rng, np.random.Generator)
