gbp.io
======

.. currentmodule:: gbp.io

Serialization for ``RawModelData`` and ``ResolvedModelData``.

Two formats are supported: **Parquet** (efficient binary columnar storage) and
**dict/JSON** (human-readable, useful for debugging and tests).

.. autosummary::
   :nosignatures:

   save_raw_parquet
   load_raw_parquet
   save_resolved_parquet
   load_resolved_parquet
   raw_to_dict
   raw_from_dict
   resolved_to_dict
   resolved_from_dict

Parquet
-------

Save and load model data as a directory of Parquet files.

.. automodule:: gbp.io.parquet
   :members:

Dict / JSON
------------

Convert model data to and from plain Python dictionaries.

.. automodule:: gbp.io.dict_io
   :members:
