gbp.build
=========

.. currentmodule:: gbp.build

Build pipeline: transforms ``RawModelData`` into ``ResolvedModelData``.

The pipeline is stateless and deterministic — given the same raw input, it
always produces the same resolved output.  The main entry point is
:func:`build_model`, which orchestrates validation, time resolution, edge
construction, lead-time computation, transformation joins, fleet capacity
calculation, and spine assembly.

.. autosummary::
   :nosignatures:

   build_model
   validate_raw_model
   ValidationResult
   ValidationError
   assemble_spines

Pipeline Orchestration
----------------------

.. automodule:: gbp.build.pipeline
   :members:

Validation
----------

Unit consistency, referential integrity, and graph connectivity checks.

.. automodule:: gbp.build.validation
   :members:

Edge Construction
-----------------

Vectorized edge materialization from facility types and rules.

.. automodule:: gbp.build.edge_builder
   :members:

Time Resolution
---------------

Map raw calendar dates to integer planning periods.

.. automodule:: gbp.build.time_resolution
   :members:

Lead Times
----------

Resolve edge lead times from hours to integer period offsets.

.. automodule:: gbp.build.lead_time
   :members:

Transformations
---------------

Join N-to-M commodity transformation definitions with facilities.

.. automodule:: gbp.build.transformation
   :members:

Fleet Capacity
--------------

Effective fleet capacity per home facility and resource category.

.. automodule:: gbp.build.fleet_capacity
   :members:

Spine Assembly
--------------

Merge resolved attribute tables onto entity bases.

.. automodule:: gbp.build.spine
   :members:
