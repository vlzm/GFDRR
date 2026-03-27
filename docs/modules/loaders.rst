gbp.loaders
============

.. currentmodule:: gbp.loaders

High-level data loaders that assemble ``RawModelData`` from external sources.

:class:`DataLoaderGraph` is the main production loader — it reads data from a
source that implements :class:`DataSourceProtocol`, builds the graph, and runs
the full build pipeline.  :class:`DataLoaderMock` generates synthetic Citi
Bike-style data for testing and experimentation.

.. autosummary::
   :nosignatures:

   GenericSourceProtocol
   BikeShareSourceProtocol
   DataSourceProtocol
   GraphLoaderProtocol
   GraphLoaderConfig
   DataLoaderGraph
   DataLoaderMock

Protocols
---------

Structural protocols that data sources must implement.

.. automodule:: gbp.loaders.protocols
   :members:

Contracts
---------

Configuration and validation schemas for loaders.

.. automodule:: gbp.loaders.contracts
   :members:

Graph Loader
------------

Assemble ``RawModelData`` from a data source and run ``build_model()``.

.. automodule:: gbp.loaders.dataloader_graph
   :members:

Mock Loader
-----------

Generate Citi Bike-like temporal mock data for testing.

.. automodule:: gbp.loaders.dataloader_mock
   :members:
