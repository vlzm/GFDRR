# GBP — Graph-Based Logistics Platform

A universal graph-based logistics platform for network flow problems,
built on multi-commodity flow formulation. Domain-agnostic data model
validated against bike-sharing (Citi Bike-style).

**Pipeline:** Raw Data → `RawModelData` → `build_model()` → `ResolvedModelData` → Consumer (Environment / Optimizer / Analytics).

**Source:** [github.com/vlzm/GFDRR](https://github.com/vlzm/GFDRR)

## Package Reference

```{toctree}
:maxdepth: 1

modules/core
modules/build
modules/loaders
modules/loading
modules/io
modules/rebalancer
```
