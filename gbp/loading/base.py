"""Abstract data source protocol for producing ``RawModelData``."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from gbp.core.model import RawModelData


class DataSourceProtocol(Protocol):
    """Any object that can produce a ``RawModelData``."""

    def load(self) -> RawModelData:
        """Load raw data and return ``RawModelData``."""
        ...
