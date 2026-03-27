gbp.rebalancer
==============

.. currentmodule:: gbp.rebalancer

.. warning::

   Early prototype — will be redesigned in a future phase.

Bike-sharing rebalancing solver built on OR-Tools.  Formulates the problem as
a Pickup and Delivery Problem (PDP) and solves it with a vehicle routing
engine.

.. autosummary::
   :nosignatures:

   Rebalancer
   DataLoaderRebalancer
   RebalancerConfig

Rebalancer Pipeline
-------------------

.. automodule:: gbp.rebalancer.pipeline
   :members:

Data Loader
-----------

Build PDP solver inputs from graph loader output.

.. automodule:: gbp.rebalancer.dataloader
   :members:

Contracts
---------

Configuration and schemas for the rebalancer.

.. automodule:: gbp.rebalancer.contracts
   :members:

Demand Calculator
-----------------

.. automodule:: gbp.rebalancer.demand
   :members:

Routing — Solver
----------------

.. automodule:: gbp.rebalancer.routing.solver
   :members:

Routing — VRP
--------------

.. automodule:: gbp.rebalancer.routing.vrp
   :members:

Routing — Postprocessing
------------------------

.. automodule:: gbp.rebalancer.routing.postprocessing
   :members:
