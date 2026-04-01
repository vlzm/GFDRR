"""Loaders package — data loading, graph construction, CSV utilities."""

from .contracts import GraphLoaderConfig
from .csv_loader import CsvLoader, load_csv_folder
from .dataloader_graph import DataLoaderGraph
from .dataloader_mock import DataLoaderMock
from .protocols import (
    BikeShareSourceProtocol,
    GenericSourceProtocol,
    GraphLoaderProtocol,
)

__all__ = [
    "BikeShareSourceProtocol",
    "CsvLoader",
    "DataLoaderGraph",
    "DataLoaderMock",
    "GenericSourceProtocol",
    "GraphLoaderConfig",
    "GraphLoaderProtocol",
    "load_csv_folder",
]
