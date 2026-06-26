---
name: phase-diagram
description: >
  Draw a single, code-verified mermaid sequence diagram of simulator phases
  (the per-period loop) in the project's approved format: bike pools as
  participants, and a four-part spec per phase (Reads / Mechanics / Writes /
  Invariant). Use this skill whenever the user asks to visualize simulation
  phases or the simulation loop: "нарисуй диаграмму фаз", "диаграмма
  симуляции", "визуализируй фазы", "draw the phase diagram", "diagram for
  these phases", or gives a list of Phase classes (DockArrivals,
  FormDeparturesPhase, ...) and wants to see how they work. Also trigger on
  "как работает симуляция, покажи диаграммой" and similar. The output format
  here is the result of several feedback rounds — follow it exactly, do not
  improvise a different diagram type.
---

# Phase Diagram Skill

## Purpose

Produce ONE mermaid `sequenceDiagram` that explains the per-period logic of a
given list of simulator phases. The reader is the developer who wrote the code
but wants to *see* the choreography: what each phase reads, which mechanics it
applies, what it writes, and which conservation invariant must hold.

The format below was converged on through several rounds of user feedback.
Every rule exists because its violation was explicitly rejected. Follow them
exactly.

## Input

A list of phase constructor calls, e.g.:

```python
[DockArrivals("previous"), FormDeparturesPhase(), FormPotentialTripsPhase(), DockArrivals("same")]
```

If no list is given, use `phases_canonical` from `notebooks/test_pipeline.ipynb`.

## Workflow

1. **Read the code first.** For every phase in the list, read its `execute()`
   in `gbp/consumers/simulator/phases.py` and every mechanic it calls in
   `gbp/consumers/simulator/mechanics.py`, plus the state/journal helpers it
   touches (`gbp/consumers/simulator/state.py`, `gbp/model/flows.py`).
   Never write a signature, a read, a write, or an invariant from memory —
   every line of the diagram must be checkable against the code.
2. **Identify the pools.** Participants are the places bikes live, not code
   modules. Default: `AT STATIONS (inventory)` and `IN TRANSIT (in_transit)`.
   Add a pool (e.g. a truck pool) only if a phase in the list actually moves
   bikes through it.
3. **Build the diagram** following the format rules below.
4. **Verify and report.** After the diagram, list the nuances found while
   checking the code (silent drops, ordering subtleties, hidden invariant
   preconditions). Only real findings — no padding. If there are none, say so
   in one line.

## Format rules (hard requirements)

### Scope and shape

- **Exactly one diagram.** No companion diagrams, no end-to-end pipeline
  context, no architecture/layering views unless the user explicitly asks.
  Scope = the phase list given, nothing wider.
- **`sequenceDiagram`, never a flowchart.** Time must flow top-down so the
  within-period phase order is unambiguous (a flowchart was misread as
  "phases 1 and 4 run in parallel").
- **Participants = bike pools** (where bikes physically are), not classes.
  Arrows between pools = actual bike movements, labeled with the journal
  event they produce (`arrived`, `redirected`, `departed`). In-place changes
  (inventory decrement) are self-arrows with a short label like `−realized`.
- Wrap the phases in `loop each period t ... end`.
- Each phase sits in its own `rect rgb(...)` block. Same operation → same
  color (both `DockArrivals` instances share blue), so the symmetry is
  visible. Palette used so far: docking `rgb(235, 245, 255)`, departure gate
  `rgb(235, 255, 235)`, trip formation `rgb(255, 245, 230)`.

### Per-phase block: title + four notes

Each phase block contains, in this order:

1. `Note over ...: PHASE N — ClassName("args")` — title.
2. `📥 Reads` — every input, **grouped by source container**, one line per
   container, prefix first:
   - `state: ...` — e.g. `in_transit → due[planned_end == t, start before t]`
   - `resolved: ...` — real attribute names (`historical_demand_df`,
     `facilities_capacities_df`).
   Derived selections are written `container: table → alias[filter]`.
3. `⚙️ Mechanics` — one line per mechanic **in execution order**, in the
   notation `output = func(args)` with ALL real arguments (e.g.
   `dock_up_to_capacity(due, free)` — both args, never drop one), followed by
   a short clause of what it does. See notation rules below.
4. Movement arrows (between notes 3 and 5, where they happen logically).
5. `✍️ Writes` — every output, **grouped by target container**:
   - `state: ...` — inventory, in_transit, intermediates.
   - `SimulationLog: ...` — the journal events (`+= arrived_events(docked)`).
   Explicitly state the non-writes when they are surprising: "SimulationLog:
   nothing — no trips yet, counts only", "inventory untouched — decremented
   in phase 2".
6. `✅ Invariant` — the conservation equation(s) of the phase, e.g.
   `count(due) == docked + placed + lost` or
   `demand == realized + lost, realized ≤ stock`.

If two phases are the same class with a different filter: give the later one
an abbreviated block ("same mechanics and reads/writes as phase N,
filter: ...") but ALWAYS repeat its invariant.

After the loop, add a run-level invariant note if the scenario has one (base
replay: `lost == 0 everywhere → simulated_departures_df == historical_departures_df`).

### Notation

- Tuples of return values: `(docked, overflow) = dock_up_to_capacity(due, free)` —
  comma-separated, NEVER `docked + overflow` (`+` was misread as addition).
- `+` and `−` only for real arithmetic that exists in the code.
- Cardinalities: `count(x)`.
- Comparison: `==`, `≤`, `≥`; write words (`before t`, `less than`) instead
  of `<`/`>`.
- Modified-copy variables: a prime mark, e.g. `inventory´ — already includes docked`.
- OD probabilities: `P(target | source, commodity)`.
- Shortened names are allowed when introduced by the full name once
  (`state.inventory` for `state.state_inventory_df`).
- `SimulationLog` = `state.state_flows_df`, kept as a separate write target on
  purpose: phases return events in `PhaseResult.events`, the engine appends
  them to the journal.

### Language

Everything inside the diagram — participant labels, notes, arrow text — in
**English only**, no Russian. The diagram is an artifact, so it follows the
same rule as code and docstrings. The chat text around the diagram stays
Russian as usual.

### Mermaid syntax safety (hard-won, do not skip)

- Start with the init directive:
  `%%{init: {"sequence": {"wrap": true, "noteAlign": "left", "actorMargin": 420, "noteMargin": 12}}}%%`
  (`wrap: true` prevents text overflowing the note boxes, `noteAlign: left`
  makes spec text readable, `actorMargin` widens the note span).
- **No semicolons `;` anywhere in note or message text** — mermaid treats them
  as statement separators and the parser dies. Use commas or `<br/>`.
- **No `<` or `>` characters in text** — they read as HTML tags. Use `≤`, `≥`,
  or words.
- Use `<br/>` for logical line breaks; `wrap: true` catches overly long lines.
- Emoji anchors `📥 ⚙️ ✍️ ✅` at the start of each spec note — they are the
  visual navigation, keep them.

## Reference output

[example.md](example.md) holds the approved diagram for the canonical four
phases, exactly as accepted by the user. Match its look and density: new
diagrams for other phase lists must be recognizably the same format.
