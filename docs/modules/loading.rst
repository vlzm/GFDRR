gbp.loaders — CSV utilities
===========================

.. currentmodule:: gbp.loaders

:class:`CsvLoader` reads a folder of CSV files (one per table) and validates
columns against expected schemas.

.. autosummary::
   :nosignatures:

   CsvLoader
   load_csv_folder

CSV Loader
----------

Load a folder of CSV files into ``RawModelData``.

.. automodule:: gbp.loaders.csv_loader
   :members:

Validators
----------

Column validation helpers for loaded DataFrames.

.. automodule:: gbp.loaders.validators
   :members:
