"""Exceptions raised by the simulator consumer."""

from __future__ import annotations


class SimulatorConfigError(ValueError):
    """Resolved model lacks the inputs the simulator needs to run."""
