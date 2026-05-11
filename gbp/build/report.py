"""Diagnostic report produced by ``build_model``.

Tracks which tables were auto-derived by the build pipeline when the user
did not supply them in ``RawModelData``.  This keeps the "manual wins"
contract auditable: after a build, the caller can inspect
``ResolvedModelData.build_report`` to see what was filled in and why.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class BuildReport:
    """Record of derivations performed by ``build_model``.

    Attributes
    ----------
    derivations : dict[str, str]
        Mapping from table name to a short human-readable reason string
        (e.g. ``"derived from observed_flow"``). Default is an empty dict.
    """

    derivations: dict[str, str] = field(default_factory=dict)

    def add(self, table: str, reason: str) -> None:
        """Record that *table* was auto-derived with the given *reason*.

        Parameters
        ----------
        table
            Name of the derived table.
        reason
            Human-readable derivation rationale.
        """
        self.derivations[table] = reason

    def is_empty(self) -> bool:
        """Return ``True`` when no derivation was recorded.

        Returns
        -------
        bool
        """
        return not self.derivations
