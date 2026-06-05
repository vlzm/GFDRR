# Citi Bike Simulation Platform

## Vision

A bike-sharing simulation platform built vertically on the Citi Bike domain. The core is **Environment**: bikes move through a network of stations across time periods. Trips occur, inventory changes, rebalancing tasks run.

The platform is developed **vertically**: one domain at a time. Citi Bike is the first domain. Everything in the codebase must serve this domain. Expansion to other domains (gas logistics, block logistics, etc.) will happen later, as separate verticals.

---

## Current Goal

**Minimal working scenario.**

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
| `notebooks/test_pipeline.ipynb` | The reference scenario — source of truth for what must work |
| `CLAUDE.md` | AI collaboration rules |
