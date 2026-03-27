# GBP — Graph-Based Logistics Platform

A universal graph-based logistics platform for network flow problems,
built on multi-commodity flow formulation. Domain-agnostic data model
validated against bike-sharing (Citi Bike-style).

---

## Data Pipeline

```{raw} html
<div class="pipeline">
  <span class="pipeline-step">Raw Data</span>
  <span class="pipeline-arrow">&#x2192;</span>
  <span class="pipeline-step"><code>RawModelData</code></span>
  <span class="pipeline-arrow">&#x2192;</span>
  <span class="pipeline-step"><code>build_model()</code></span>
  <span class="pipeline-arrow">&#x2192;</span>
  <span class="pipeline-step"><code>ResolvedModelData</code></span>
  <span class="pipeline-arrow">&#x2192;</span>
  <span class="pipeline-step">Consumer</span>
</div>
```

The pipeline is **stateless and deterministic**: given the same raw input,
it always produces the same resolved output.

---

## Core Concepts

Three pillars of the data model:

Facility — WHERE
: Nodes of the graph. Each has a type, operations, and derived roles
  (SOURCE, SINK, STORAGE, TRANSSHIPMENT).

Commodity — WHAT
: The substance that flows through the network. L2 categories are
  domain-agnostic; optional L3 instances track concrete items.

Resource — HOW
: Transport vehicles/channels that move commodity along edges.
  Capacity and compatibility are tracked per resource category.

---

## Package Reference

```{raw} html
<div class="card-grid">
  <a class="card" href="modules/core.html">
    <h3>Core Data Model</h3>
    <p>Facilities, commodities, resources, edges &mdash; the three pillars
    plus attribute system and spine assembly.</p>
  </a>
  <a class="card" href="modules/build.html">
    <h3>Build Pipeline</h3>
    <p>8-step stateless pipeline: validation, time resolution,
    edge construction, lead times, and spine assembly.</p>
  </a>
  <a class="card" href="modules/loaders.html">
    <h3>Data Loaders</h3>
    <p>Domain-specific sources to universal graph model.
    Mock data generator for bike-sharing validation.</p>
  </a>
  <a class="card" href="modules/io.html">
    <h3>Serialization</h3>
    <p>Parquet and JSON round-trip I/O for RawModelData
    and ResolvedModelData.</p>
  </a>
  <a class="card" href="modules/loading.html">
    <h3>Loading</h3>
    <p>CSV loading utilities and data source protocols.</p>
  </a>
  <a class="card" href="modules/rebalancer.html">
    <h3>Rebalancer</h3>
    <p>Early prototype PDP solver using OR-Tools for
    bike-sharing domain validation.</p>
  </a>
</div>
```

---

## Architecture & Design

```{raw} html
<div class="card-grid">
  <a class="card" href="architecture_diagrams.html">
    <h3>Architecture Diagrams</h3>
    <p>12 progressive Mermaid diagrams covering entities,
    edges, temporal model, build pipeline, and more.</p>
  </a>
  <a class="card" href="DATA_JOURNEY.html">
    <h3>Data Journey</h3>
    <p>End-to-end walkthrough: from raw bike data through
    build_model() to optimizer constraints.</p>
  </a>
  <a class="card" href="design/graph_data_model.html">
    <h3>Data Model Spec</h3>
    <p>Complete table-level specification of all entities,
    grains, and relationships.</p>
  </a>
  <a class="card" href="design/attribute_system.html">
    <h3>Attribute System</h3>
    <p>Dynamic registry for parametric data: specs, grains,
    groups, and spine assembly.</p>
  </a>
</div>
```

---

**Source:** [github.com/vlzm/GFDRR](https://github.com/vlzm/GFDRR)

```{toctree}
:maxdepth: 1
:hidden:

modules/core
modules/build
modules/loaders
modules/loading
modules/io
modules/rebalancer
```

```{toctree}
:maxdepth: 1
:caption: Architecture
:hidden:

architecture_diagrams
DATA_JOURNEY
design/graph_data_model
design/attribute_system
design/environment_design
design/refactoring
```
