"""Model layer: the shared domain vocabulary the whole system speaks.

The flow journal (:mod:`gbp.model.journal`) is the central abstraction of the
platform -- the format in which both the loaders (historical flows) and the
simulator (simulated flows) express what happened, and from which every marginal
observation is derived. It lives here, in its own layer, rather than inside any
one producer or consumer: the loaders and the simulator both depend on it, and
it depends on neither.
"""
