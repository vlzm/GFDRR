gbp.loading
===========

.. currentmodule:: gbp.loading

Low-level data source layer: read raw files into ``RawModelData``.

:class:`CsvLoader` reads a folder of CSV files (one per table) and validates
columns against expected schemas.  Implements :class:`DataSourceProtocol`
which can be plugged into higher-level loaders from :mod:`gbp.loaders`.

.. autosummary::
   :nosignatures:

   DataSourceProtocol
   CsvLoader
   load_csv_folder

Data Source Protocol
--------------------

Abstract interface that all data sources implement.

.. automodule:: gbp.loading.base
   :members:

CSV Loader
----------

Load a folder of CSV files into ``RawModelData``.

.. automodule:: gbp.loading.csv_loader
   :members:

Validators
----------

Column validation helpers for loaded DataFrames.

.. automodule:: gbp.loading.validators
   :members:
