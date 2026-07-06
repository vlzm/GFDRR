# Levels of code understanding

> Russian version: [`comprehension_levels_ru.md`](comprehension_levels_ru.md).
> Keep the two files in sync.

## Why this document exists

When a developer writes code line by line, they understand what it does as they
go. When an AI assistant (Claude Code, Copilot, Cursor) writes the code and the
human only reads and accepts it, a gap appears between **how much code exists in
the system** and **how much of it the human actually understands**.

This gap is now called **comprehension debt** (or **cognitive debt**). It is
dangerous because it grows quietly: tests pass, the code looks clean, the
metrics are good — but nobody can explain why the system is built the way it
is. When the moment comes to change or fix something, it turns out there is
nobody who can.

To discuss this gap concretely, it helps to distinguish **levels of
understanding**. Not all understanding of code is equally deep, and not every
place needs the maximum depth. Below are five levels, from the most shallow to
the deepest.

---

## Level 1. Interface user

**What the person knows:** how to use the system from the outside. What it can
do, what it cannot, how to run it, how to pass data in, what comes out.

**Analogy:** a person who drives a car. Knows where the pedals are, how to turn
on the lights, that it needs fuel. Knows nothing about how the engine works.

**Example for this project:**
"I run a scenario with `python app/runner.py`, it saves a run folder under
`data/runs/<run_name>/`, and I open `streamlit run app/main.py` to look at the
maps and charts. If something goes wrong, I read the terminal output."

**When it is enough:** an end user, an analyst, a manager. Everyone needs this
level without exception — without it a person simply cannot use the system.

---

## Level 2. Top-level modules and their coarse contracts

**What the person knows:** which large parts the system consists of and, in
rough terms, how data moves between them — what one part gives another in
domain terms, without field or type details. These are already contracts
between modules, but coarse ones: "what goes in and what comes out", not "the
exact signature of a method".

**Analogy:** the person knows a car has an engine, a gearbox, brakes,
electrics. Knows the engine takes fuel and air and gives rotation; the gearbox
takes rotation and gives rotation at a different speed. These are contracts —
but in terms of "fuel and rotation", not "the compression stroke lasts N
degrees of crankshaft rotation".

**Example for this project:**
"The system has three big blocks: the simulator (`gbp/consumers/simulator/`)
plays a scenario period by period and produces the flow journal
([Notations.md §0](../Notations.md#0-the-flow-event-schema-the-symbol-table)) —
the table of everything that happened to every bike; the artifact builder
(`app/artifacts.py`) turns a finished run into a folder of tables under
`data/runs/<run_name>/`
([Notations.md §12](../Notations.md#12-run-artifacts-the-files-the-ui-reads));
the Streamlit UI (`app/`) reads those tables and draws them." What exactly is
inside the flow journal — at this level the person does not know.

**When it is enough:** the team lead of a neighboring team, an architect of an
adjacent system, a new team member in the first days, a stakeholder with a
technical background.

---

## Level 3. All modules of the system and their coarse contracts

**What the person knows:** the full module map of the system, including
internal and helper modules, and the coarse contracts between them — what each
module takes and gives, in general terms. Still does not know *how* each module
works inside or the exact signatures of its functions.

In essence this extends level 2 downward: the same coarse contracts, but over
all modules, not just the top ones.

**Analogy:** a mechanic who knows every unit of the car — not only the engine
and gearbox but also the cooling system, the fuel injection, ABS, the sensors.
Knows what connects to what and what each unit passes on ("the pump feeds fuel
under pressure to the rail, the rail distributes it to the injectors"), but
cannot take each unit apart into pieces and does not know the exact parameters.

**Example for this project:**
The person knows the simulator splits into the state (`state.py`: the
inventory, the bikes in transit, the journal), the rules (`mechanics.py`), the
phases (`phases.py`), the loop (`engine.py`), and rebalancing
(`rebalancing.py`). Knows the coarse contracts: mechanics take plain tables and
return decisions — which flows fit into the docks and which overflow; phases
apply those decisions to the state and write events into the journal; the
engine runs the phases in a fixed order once per period. Knows the artifact
builder has one build function per saved table (`build_arcs`,
`build_flow_totals`; the panel comes from the model read-model `flows_to_panel`). Can draw the full module diagram with labeled arrows. But
which columns the flow journal has and what their types are — at this level
still unknown.

**When it is enough:** a senior developer on the team who can navigate the
whole project but actively works only in some of its parts. This level is
enough to decide *where* in the system a change belongs, but not *how exactly*
to make it.

---

## An aside: how contracts get sharper from 2 to 4a

One thing is worth stating explicitly, because it is easy to get confused
otherwise.

Contracts between modules exist at every level starting from level 2. The
difference between the levels is not "whether there is a contract" but **how
precisely it is stated and over how many modules**.

- **Level 2:** coarse contracts over the top modules. "The simulator hands the
  flow journal to the artifact builder." In domain terms, no field details.
- **Level 3:** the same coarse contracts, but over all modules, including
  internal ones. "`dock_up_to_capacity` takes the flows due to dock and the
  free-dock counts, and splits them into the ones that fit and the overflow."
- **Level 4a:** the same contracts, sharpened to a full technical
  specification. "`dock_up_to_capacity(due: pd.DataFrame, free: pd.Series,
  target_col: str = 'planned_target_id') -> tuple[pd.DataFrame, pd.DataFrame]`;
  within each target the first `free` flows in row order dock; no target docks
  more flows than it has free slots."

So 2 → 3 widens the coverage (more modules), and 3 → 4a deepens the precision
(a full technical specification instead of general words). At no point does a
contract appear "for the first time" — it only becomes wider or sharper.

---

## Level 4. Inside a module — classes, methods, contracts

In practice this level splits into two sublevels. The split matters: it is the
one most often confused, and it is the main weak point of AI coding.

### Level 4a. Full technical contracts inside a module

At levels 2 and 3 the contracts already exist — but coarse, in "what goes in
and what comes out" terms. Level 4a sharpens these contracts to the precision
of the actual code: signatures, types, invariants, exceptions, edge cases.

**What the person knows:** which classes and functions the module has, their
exact signatures with parameter and return types, which invariants hold before
and after a call (what is guaranteed on entry, what is guaranteed on exit),
which exceptions are raised and when, what happens on empty or boundary
inputs. This is the level of contract usually written down in an OpenAPI spec,
a pydantic schema, or a carefully written docstring.

**Analogy:** the mechanic has read the technical documentation of one unit —
say, the fuel injection system. Knows the fuel pump delivers 3 to 5 bar (below
that there is no injection, above it the seals fail), that an injector fires on
a pulse of such-and-such length, that when the pressure sensor fails the
control unit switches to a fallback mode. The same "fuel and rotation" as at
level 2, but with numbers and exact conditions.

**Example for this project:**
At level 2 it was: "the artifact builder turns a run into a folder of tables".
At level 4a it is the exact contract of that folder
([Notations.md §12](../Notations.md#12-run-artifacts-the-files-the-ui-reads)):
`meta.json` holds the run parameters, the invariant `violations` list, and the
whole-run totals; `flows.parquet` is the finalized journal with the measure
columns; `panel.parquet` has one row per
`(period_id, facility_id, commodity_category)`; `arcs.parquet` has one row per
`(flow_id, move_id)` pair. Inside the rebalancer
([`rebalancing.md`](rebalancing.md)) there is
`solve_rebalance_vrp(nodes, travel_minutes, trucks, params)` returning the
stops table, with these guarantees: every truck starts and ends empty at its
home depot, the truck's load never goes below zero or above its capacity, every
route fits into `params.window_minutes`, and a node that does not fit is
skipped — it simply does not appear in the answer.

**When it is enough:** a developer who writes code that uses this module as a
library. They do not need to know *how* `solve_rebalance_vrp` routes the
trucks — they need the exact contract, to call it correctly and handle the
answer correctly.

### Level 4b. The theory of the module — why it is built this way

The most undervalued level. This is where the "theory of the system" lives, in
the sense Peter Naur wrote about in 1985: a program is not just source code, it
is a *theory in the developers' heads* — the reasons behind the decisions.

**What the person knows:**
- *why* this module has this structure and not another;
- which alternatives were considered and why they were rejected;
- which design decisions are load-bearing — they cannot be changed without
  rethinking the whole architecture;
- which decisions are arbitrary — they can be changed and nothing breaks;
- which invariants are critical and which are accidental properties of the
  current implementation.

**Analogy:** the design engineer who knows *why* the injection system has a
pump of exactly this power: because the engine is sized for such-and-such
volume, because emission rules require this pressure, because the alternative
was rejected on cost. He understands which parameters are fixed by physics and
which are just historical accident.

**Example for this project:**
The developer knows why the run's history is a flow journal — an append-only
table of events — rather than a mutable state table: every read-model
(inventory, demand, the panel) is computed from the journal, so the simulator
and the historical loader can be compared row by row. This is a load-bearing
decision; it is written down in the "Why it is built this way" section of
[`simulator.md`](simulator.md#why-it-is-built-this-way). Knows why rebalancing
is planned once per window but executed period by period, re-checking free
docks and bikes on hand at every step instead of trusting the plan
([`rebalancing.md`](rebalancing.md#why-it-is-built-this-way)). And knows that
the solver counting time in tenths of a minute (`_MINUTE_SCALE` in
`rebalancing.py`) is an arbitrary decision: OR-Tools works in whole numbers,
and tenths are precise enough — hundredths would work just as well, nothing
would break.

**When it is enough:** the module owner, the tech lead, the person who will
modify this module in the future. Without this level the code cannot be
changed safely — it is unclear which changes break the architecture and which
are harmless.

**Why 4b matters especially with AI coding:** an AI assistant can generate code
that *works* and *passes the tests*, but is built on arbitrary decisions that
look load-bearing. Six months later, when something has to change, nobody
remembers which of those decisions can be touched and which cannot. The system
becomes fragile.

---

## Level 5. Line-by-line implementation

**What the person knows:** how each line of code in the module actually works.
What this loop does, why this exact check is here, what happens on an empty
input, why the DataFrame is copied here instead of passed by reference.

**Analogy:** the mechanic who has taken this unit apart himself. Has seen how
each spring is made, knows where the ring can leak, knows why this bolt is
tightened to exactly this torque.

**Example for this project:**
The developer can walk through `plan_overflow_redirect()` in `mechanics.py` and
explain: "The loop here runs in rounds — why? Each round of same-period
dockings takes docks, and the next round must see them taken; the local copy of
the inventory (`running`) tracks that without touching the live state. A leg
that takes time is not docked now — why? Whether it fits is decided in the
period it arrives, where it can bounce again. The `assert` at the end — why?
Every overflow flow must either get a new leg or be lost, never both; if the
two sides do not add up to the input, the function has silently dropped or
duplicated a bike."

**When it is mandatory:** in code with **silent failure modes** — where a
mistake gives no signal and just quietly corrupts the result. That is:

- **Numerical and algorithmic correctness:** optimizers, numerical methods,
  statistics. A mistake raises no exception — the answer is simply wrong.
- **Concurrency and transaction invariants:** the mistake shows up only under
  load, once a month, and corrupts data.
- **Security and cryptography:** the mistake looks like working code but
  leaves a hole.
- **Deep modules** (in Ousterhout's sense) — modules with a narrow interface
  and complex internals. Their danger: from the outside they look simple, and
  mistakes inside are not visible from the outside.
- **Code you will actively change** in the coming weeks or months.
- **The key abstractions of the system** — what dozens of other modules depend
  on.

**When it can be postponed:** stable code that does not change; code well
covered by tests of *semantics* (not just "does not crash" but "gives the
right answer on different inputs"); code for which a second person holds
level 5; one-off prototypes and research code.

**What cannot be done:** level 5 cannot be *skipped*. It can be postponed. It
can be split between people on the team. But the moment this code has to
change, someone will have to walk it at level 5. This is manual work; it cannot
be saved — only moved in time.

---

## How the levels relate to each other

The levels are not a strict ladder that must be climbed bottom to top. An
important observation: **level 5 does not automatically give level 4b**. This
is counterintuitive but essential.

You can read AI-generated code line by line, understand what every line does
(level 5) — and still not know *why* this data structure was chosen, which
alternatives were rejected, which decisions are load-bearing. This is exactly
the state of the developer who "reviewed the code, all good".

The reverse also holds: you can have a 4b understanding of a module from a
design document — know the intent, the alternatives, the reasons — and be
unable to reproduce a single concrete line of code. This state works until you
have to debug.

**Solid understanding of code exists only when both 4b and 5 are there at the
same time.** Theory without implementation is belief, not knowledge.
Implementation without theory is the ability to repair a car without
understanding what it is for.

---

## Which level is needed where

| Role | Required minimum |
|------|------------------|
| End user | 1 |
| Manager, analyst | 1 |
| Stakeholder with a technical background | 2 |
| New developer (first week) | 2, moving to 3 |
| Developer of an adjacent module | 3 + 4a of the module they integrate with |
| Senior developer on the team | all of 3, 4a over all modules, 4b + 5 in their own areas |
| Module owner | 4a + 4b + 5 for their module, in full |
| Tech lead / architect | 2, 3, 4a in full; 4b for all critical modules; 5 for the system core |
| Solo developer of a complex system | 4a + 4b + 5 in full, over the whole system |

The last row is a special case. If one person builds the project, and it has
critical components (optimizers, systems with silent failure modes, deep
modules), there is nobody to share level 5 with. That person must hold it
alone — otherwise comprehension debt makes the system unchangeable within a
few months.

---

## Practical consequences for working with AI assistants

The main danger of AI coding is the illusion that one can work effectively at
level 4a while skipping 4b and 5. The AI assistant generates code fast, the
code looks right, tests pass. From the outside everything is fine. But six
months later, when the moment comes to change this code, it turns out nobody
can explain why it is built this way.

What follows from this:

**First**, before using AI to generate code, decide explicitly which level of
understanding this piece needs. For a one-off prototype 4a is enough — it works
and is forgotten. For the system core level 5 is needed, and that is not a
compromise.

**Second**, 4b (the theory of the module) must be written down — in design
decision records, specifications, design documents. In this repository it
lives in the "Why it is built this way" sections of
[`simulator.md`](simulator.md#why-it-is-built-this-way) and
[`rebalancing.md`](rebalancing.md#why-it-is-built-this-way). Otherwise it fades
with time even in the author's head, let alone in other people's.

**Third**, if for some module you decided to work at 4a without 5 — that must
be a conscious choice, made knowing the debt will have to be paid some day. Not
an accidental "good enough".

**Fourth**, AI assistants are useful not only for generating code but also for
*building level 5* — when the developer talks through their mental model and
the assistant asks questions that expose the holes in the understanding. This
is a fundamentally different mode of use, and it preserves understanding
instead of washing it out.

---

## The point in one sentence

There are five levels of understanding (with level 4 split into 4a and 4b),
different roles need different depth, and the main trap of AI coding is
thinking you can live at the shallow levels without paying for it later. You
will pay anyway — just later, and with interest.
