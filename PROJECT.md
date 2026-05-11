# Graph-Based Logistics Platform

## Vision

A platform for modeling and optimizing logistics networks. The core is **Environment**: a space where commodities (bikes, goods, money) move through a network of facilities across time periods. Inside the Environment, tasks run (rebalancing, repair, dispatch), decisions are made, and the world state is updated.

The data model is domain-agnostic, built on a multi-commodity flow formulation (Williamson, Ahuja/Magnanti/Orlin). Tabular structures for pandas/PySpark. Parameters are managed via `AttributeRegistry` with custom grains.

The first domain is bike-sharing (Citi Bike-style).

### Two Levels of Tasks

**Operational (Environment)** — step-by-step simulation. The Environment advances through periods: period 0 → 1 → 2... At each step: trips occur, inventory is updated, tasks run (rebalancer at night, repair in the morning). The world state changes after each step. This is a digital twin: "what happens each day".

**Strategic (Optimizer)** — a one-shot solve. Takes a year of data (all potential trips, all costs), formulates an LP/MILP, and the solver minimizes the cost function. No step-by-step process — all periods are visible at once. This is strategic planning: "how many trucks to buy, where to place depots".

Both levels use the same `ResolvedModelData` but process it differently.

---

## Roadmap

1. **Foundation** — data model, build pipeline, loader
2. **Environment** — step-by-step engine, state management
3. **Rebalancer** — first task inside Environment (VRP)
4. **Implement real data instead of mock data** — at this stage we take real data from "data\raw\202602-citibike-tripdata_1.csv" and create dataloader_raw and dataloader_graph for real data
5. **Create FastAPI for future UI** — first we need to define what we want to see in the UI, then build the FastAPI based on that understanding
6. **UI** — Environment visualization (Streamlit or Gradio or React)
7. **Infrastructure** — DB, API, Docker, CI/CD
8. **Cloud** — Azure deployment

Each phase starts with a design doc. Current progress is in `PROJECT_STATE.md`.

---

## Principles

**Minimalism (Nano-style).** Code must be hackable. No model factories, heavy DI containers, or hidden magic. Every file should be understandable in 5 minutes.
**Vectorization first.** All math via pandas/NumPy. No `for` loops over data in hot paths.
**Design doc before code.** Every new subsystem starts with a design doc, discussion, and only then — implementation.
**Strict typing.** Pydantic for all contracts. Type hints on all public functions.
**English in code, Russian in chat.** Code, comments, docstrings — English only. Communication with the user — in Russian.

---

## Key Documents

| Document | Purpose | Update Frequency |
|----------|---------|------------------|
| `PROJECT.md` | Vision, roadmap, principles | Rarely (when the vision changes) |
| `PROJECT_STATE.md` | Current phase, progress, "not now" | At phase transitions and within phases |
| `docs/design/` | Design docs per subsystem | One per phase |
