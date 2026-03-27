gbp.core
========

.. currentmodule:: gbp.core

Core graph logistics data model (L2 + bike-sharing L3 defaults).

This package defines the central data contracts used throughout the platform:
two container dataclasses (:class:`RawModelData` and :class:`ResolvedModelData`),
all domain enumerations, Pydantic row schemas for every table, and the
attribute system that drives parametric spine assembly.

.. autosummary::
   :nosignatures:

   RawModelData
   ResolvedModelData
   make_raw_model
   FacilityType
   FacilityRole
   ModalType
   OperationType
   PeriodType
   AttributeKind
   AttributeSpec
   AttributeRegistry
   AttributeBuilder
   derive_roles

Model Containers
----------------

The two main dataclasses that hold all tabular data.
``RawModelData`` is the user-facing input; ``ResolvedModelData`` is what
``build_model()`` produces for downstream consumers.

.. automodule:: gbp.core.model
   :members:

Enumerations
------------

Domain-agnostic (L2) enums and bike-sharing (L3) defaults.

.. automodule:: gbp.core.enums
   :members:

Facility Roles
--------------

Roles derived from facility type and operations.

.. automodule:: gbp.core.roles
   :members:

Factory
-------

Quick-start factory for creating a valid ``RawModelData`` from minimal inputs.

.. automodule:: gbp.core.factory
   :members:

Attribute System
----------------

Specs, grain groups, merge planning, spine builder, and the central registry.

.. automodule:: gbp.core.attributes.spec
   :members:

.. automodule:: gbp.core.attributes.registry
   :members:

.. automodule:: gbp.core.attributes.builder
   :members:

.. automodule:: gbp.core.attributes.grain_groups
   :members:

.. automodule:: gbp.core.attributes.merge_plan
   :members:

.. automodule:: gbp.core.attributes.defaults
   :members:

Schemas — Entities
------------------

Row schemas for facilities, commodities, and resources.

.. automodule:: gbp.core.schemas.entity
   :members:

Schemas — Edges
---------------

Edge definition and edge-level attributes.

.. automodule:: gbp.core.schemas.edge
   :members:

Schemas — Temporal
------------------

Planning horizon, segments, and period definitions.

.. automodule:: gbp.core.schemas.temporal
   :members:

Schemas — Demand & Supply
-------------------------

Demand, supply, and inventory boundaries.

.. automodule:: gbp.core.schemas.demand_supply
   :members:

Schemas — Behavior
------------------

Facility behavior, operations, availability, and edge rules.

.. automodule:: gbp.core.schemas.behavior
   :members:

Schemas — Parameters
--------------------

Operation, transport, and resource cost parameters.

.. automodule:: gbp.core.schemas.parameters
   :members:

Schemas — Resources
-------------------

Resource compatibility, fleet, availability, and costs.

.. automodule:: gbp.core.schemas.resource
   :members:

Schemas — Transformations
-------------------------

N-to-M commodity transformation definitions.

.. automodule:: gbp.core.schemas.transformation
   :members:

Schemas — Hierarchy
-------------------

Facility and commodity hierarchy structures.

.. automodule:: gbp.core.schemas.hierarchy
   :members:

Schemas — Pricing
-----------------

Tiered commodity pricing.

.. automodule:: gbp.core.schemas.pricing
   :members:

Schemas — Scenarios
-------------------

Scenario configuration and overrides.

.. automodule:: gbp.core.schemas.scenario
   :members:

Schemas — Output
----------------

Optimizer and simulator output tables.

.. automodule:: gbp.core.schemas.output
   :members:
