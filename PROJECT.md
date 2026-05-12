# Citi Bike Simulation Platform

## Vision

A bike-sharing simulation platform built vertically on the Citi Bike domain. The core is **Environment**: bikes move through a network of stations across time periods. Trips occur, inventory changes, rebalancing tasks run.

The platform is developed **vertically**: one domain at a time. Citi Bike is the first domain. Everything in the codebase must serve this domain. Expansion to other domains (gas logistics, block logistics, etc.) will happen later, as separate verticals.

**Pipeline:** Raw Citi Bike data → `DataLoaderGraph` → `RawModelData` → `build_model()` → `ResolvedModelData` → `Environment` → `SimulationLog`.

---

## Current Goal

**Minimal working scenario.** The codebase must contain exactly what is needed to run `notebooks/canonical_scenario.ipynb` — nothing more, nothing less. Every module, class, function, schema field, and table that is not exercised by the canonical scenario should be removed.

This is a cleanup phase. The repository was previously built horizontally (domain-agnostic), which led to unused abstractions, dead code, and untested paths. The cleanup reduces the codebase to a verified, working vertical slice.

---

## Principles

- **Vertical, not horizontal.** No "domain-agnostic" abstractions. If Citi Bike doesn't need it, delete it.
- **Minimalism.** Code must be hackable. No factories, heavy DI containers, or hidden magic.
- **Vectorization first.** All math via pandas/NumPy. No `for` loops over data in hot paths.
- **Strict typing.** Pydantic for all contracts. Type hints on all public functions.
---

## Key Documents

| Document | Purpose |
|----------|---------|
| `PROJECT.md` | Vision and principles |
| `PROJECT_STATE.md` | Current phase and progress |
| `notebooks/canonical_scenario.ipynb` | The reference scenario — source of truth for what must work |
| `CLAUDE.md` | AI collaboration rules |
