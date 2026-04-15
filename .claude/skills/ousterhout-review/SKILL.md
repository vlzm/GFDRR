---
name: ousterhout-review
description: >
  Analyze code and architecture against John Ousterhout's principles from
  "A Philosophy of Software Design". Use this skill whenever the user asks to
  "review code by Ousterhout", "check module depth", "find shallow modules",
  "check information leakage", "analyze interfaces", "review architecture quality",
  "find pass-through methods", "audit module boundaries", "Ousterhout audit",
  "review by APOSD", or any variation requesting architectural analysis based on
  software design principles. Also trigger for Russian phrases: "проверь по Ousterhout",
  "проверь глубину модулей", "найди утечки информации", "аудит архитектуры",
  "проверь интерфейсы модулей". Use this skill proactively when the user asks
  for a general "code review" or "architecture review" — Ousterhout principles
  are always relevant. This skill analyzes EXISTING code, not designs or plans.
---

# Ousterhout Architecture Review

## Purpose

Analyze a codebase (or specific modules) against the principles from
John Ousterhout's "A Philosophy of Software Design" and produce actionable
recommendations for improving module boundaries, interfaces, and information hiding.

The output is a structured report with specific code references, severity ratings,
and concrete refactoring suggestions.

---

## The Principles (Checklist)

Apply these in order. Each principle has red flags to look for and questions to ask.

### 1. Module depth ratio

**Principle:** A module's value = (hidden complexity) / (interface complexity).
Deep modules have simple interfaces and complex implementations.
Shallow modules have interfaces almost as complex as their implementations.

**Red flags:**
- Class with many public methods but each is 1-5 lines
- Function whose signature (parameters + return type) is as complex as its body
- Module where reading the interface docs takes almost as long as reading the source
- File with more public functions/classes than private ones

**How to measure:**
- Count public methods/functions (interface surface)
- Count total lines of implementation behind that interface
- Ratio < 5:1 (less than 5 lines of implementation per public method) → shallow
- Look at `__init__.py` exports — how much is exposed vs. how much is hidden?

### 2. Information hiding & information leakage

**Principle:** Each module should encapsulate a design decision that could change.
Information leakage is when the same design decision is spread across multiple modules.

**Red flags:**
- Two modules that both know the same file format, wire protocol, or data layout
- Changing one implementation detail requires changes in multiple files
- Module A imports a constant/enum from module B only to pass it back to module B
- Comments saying "this must match the format in module X"
- Shared constants that encode implementation details (not domain concepts)

**Questions to ask:**
- If I change how X is stored/computed internally, how many files change?
- Are there pairs of modules that always change together?

### 3. Temporal decomposition

**Principle:** Modules should be organized by knowledge, not by execution order.
Splitting "read file" and "write file" into separate modules leaks the file format.

**Red flags:**
- Module pairs named `*_reader` / `*_writer`, `*_serializer` / `*_deserializer`
- Pipeline steps where step N knows about the internal format of step N-1's output
- Classes named after lifecycle phases: `Initializer`, `Processor`, `Finalizer`

**Better alternative:** One module that owns the format, with read and write as methods.

### 4. Define errors out of existence

**Principle:** Design the semantics so that error cases don't arise, instead of
throwing exceptions and forcing the caller to handle them.

**Red flags:**
- Methods that raise exceptions for predictable, common cases
- Callers wrapping every call in try/except
- Functions that return `Optional[X]` when they could guarantee `X` by design
- Temporal coupling: "you must call A before B, otherwise error"
- `if x is None: raise ValueError` at the start of many methods

**Questions to ask:**
- Can I change the API contract so this error case is impossible?
- Can the constructor/factory handle the validation so methods don't need to?

### 5. Pass-through methods

**Principle:** A method that adds no abstraction and just delegates to another method
is a sign of wrong decomposition.

**Red flags:**
- Method whose body is `return self.other.method(same_args)`
- Class that wraps another class, forwarding most methods unchanged
- Layer where every call passes through to the next layer with minimal transformation
- Adapter/wrapper classes with 10+ delegating methods

**Exception:** Pass-through is acceptable for Protocol/interface compliance,
or when the wrapper adds a cross-cutting concern (logging, auth, caching).

### 6. Pass-through variables

**Principle:** A variable threaded through many layers but used only at the deepest
level creates coupling between all layers.

**Red flags:**
- Parameter that appears in 3+ function signatures but is only used in the innermost
- `config` or `context` objects passed everywhere
- Arguments added "just in case" — present in signature but rarely used

**Better alternatives:** Context object, dependency injection, module-level state.

### 7. Different layer, different abstraction

**Principle:** Adjacent layers in a system should operate at different abstraction levels.
If two layers use the same concepts, one of them is unnecessary.

**Red flags:**
- Parent class and child class operating on the same data types with same granularity
- API layer that exposes the same entities as the database layer without transformation
- Two adjacent pipeline steps that both work with the same DataFrame columns

### 8. Somewhat general-purpose interfaces

**Principle:** Interfaces should be neither too specific (one use case) nor too abstract.
Design as if you have several similar-but-different users.

**Red flags:**
- Method with boolean parameter that switches between two unrelated behaviors
- Interface designed around one caller's exact needs
- Overly generic: `process(data: Any, mode: str, **kwargs)` — interface reveals nothing
- Utility class with methods that only make sense in one context

**Test:** "What is the simplest interface that covers my current needs?"

### 9. Comments and documentation

**Principle:** Comments should describe things that are NOT obvious from the code.
Interface comments describe the abstraction (what, not how).
Implementation comments explain non-obvious choices (why, not what).

**Red flags:**
- No comments on public interfaces
- Comments that repeat the code: `# increment i` / `i += 1`
- Missing "why" comments on non-obvious design decisions
- Docstrings that describe parameters without describing the abstraction

### 10. Complexity signals

**Principle:** Complexity manifests as: change amplification (one change → many files),
cognitive load (how much you must know to make a change),
unknown unknowns (you don't even know what you need to know).

**Red flags — change amplification:**
- Adding a new entity type requires touching 5+ files
- New field on a data model → changes in loader, builder, validator, serializer, tests

**Red flags — cognitive load:**
- To understand module A, you must first understand modules B, C, and D
- Non-obvious ordering requirements between function calls
- Implicit contracts between modules (not enforced by types)

**Red flags — unknown unknowns:**
- No documentation on invariants that must be maintained
- Side effects not visible from the function signature
- Global mutable state

---

## Analysis Process

When the user asks for an Ousterhout review:

### Step 1: Determine scope

Ask (or infer from context) what to analyze:
- Specific module/file? → focused review
- Specific concern (e.g., "are my modules deep enough")? → targeted principle
- Whole project? → start with the architecture overview, then drill into 2-3 worst areas

### Step 2: Read the code

Read the target files. For each module/class/function, note:
- Public interface (methods, parameters, return types)
- Implementation size
- Dependencies (imports, what it calls)
- What design decisions it hides vs. exposes

### Step 3: Analyze against the checklist

Go through each principle. For each issue found:
- **Location:** exact file, class, method
- **Principle violated:** which one, with brief explanation
- **Severity:** critical (architecture change needed), moderate (refactor), minor (cleanup)
- **Concrete suggestion:** what to do, with code sketch if helpful

### Step 4: Produce the report

Structure the report as follows:

```
## Ousterhout Architecture Review: <scope>

### Summary
- X issues found: N critical, M moderate, K minor
- Strongest area: <what's already good>
- Biggest opportunity: <where the most improvement is possible>

### Critical Issues
#### 1. <Title> — <Principle>
**Location:** `path/to/file.py`, class `ClassName`
**Problem:** <what's wrong, with specifics>
**Suggestion:** <what to change, with code sketch>

### Moderate Issues
...

### Minor Issues
...

### What's Already Good
<Call out things that ARE well-designed by Ousterhout's standards.
This matters — the review should not be only negative.>

### Recommended Priority
1. First fix this...
2. Then this...
3. Then this...
```

### Step 5: Discuss with the developer

After presenting the report, discuss trade-offs. Not every Ousterhout violation
needs fixing — some are deliberate (e.g., pass-through for Protocol compliance).
The goal is informed decisions, not dogmatic compliance.

---

## Severity Guide

**Critical** — the issue creates ongoing complexity for anyone working with the code:
- Shallow module that is the primary interface of the system
- Information leakage between modules that change frequently
- Missing abstraction that causes change amplification across 5+ files
- Temporal decomposition that splits knowledge across modules

**Moderate** — the issue adds friction but doesn't block development:
- Pass-through methods that could be eliminated by adjusting boundaries
- Errors that could be defined out of existence
- Interfaces too specific to one caller
- Missing interface comments on public APIs

**Minor** — cleanup that improves readability:
- Comments that repeat the code
- Slightly shallow helper classes
- Minor pass-through variables (2 layers, not 5)
- Overly verbose interfaces that could be simplified

---

## Anti-Patterns in the Review Itself

- **Don't be dogmatic.** Ousterhout himself says: these are guidelines, not rules.
  If a "violation" exists for a good reason, acknowledge the trade-off.
- **Don't suggest splitting everything into tiny modules.** That's the opposite
  of what Ousterhout advocates. If anything, suggest merging shallow modules.
- **Don't confuse SOLID with Ousterhout.** SRP often produces shallow modules.
  Ousterhout explicitly pushes back on this — deeper modules are better.
- **Don't forget the "what's good" section.** A review that only criticizes
  is demoralizing and less useful than one that identifies strengths too.
- **Don't recommend changes without code sketches.** Abstract advice
  ("make the interface simpler") is useless without showing what that looks like.
- **Don't ignore the central abstraction.** Ousterhout doesn't talk about this,
  but the developer is aware that the choice of central data type (the "carrier set"
  around which modules are organized) is the highest-impact design decision.
  If relevant, comment on whether the central abstraction is well-chosen.

---

## Project-Specific Context (GBP)

This skill is used in the GBP (Graph-Based Platform) project. Key architecture:

- **Central abstraction:** `ResolvedModelData` (~23 DataFrames) — the carrier set
- **Deep modules:** `build_model(raw) → resolved`, `Environment.run() → SimulationLog`
- **Core is not a module:** `gbp/core/` is a shared data contract, not an operational module
- **Pipeline:** Raw Data → `RawModelData` → `build_model()` → `ResolvedModelData` → Consumer

When reviewing GBP code, also check:
- Does a new module respect the Raw=declarative / Resolved=materialized boundary?
- Does it read from ResolvedModelData without leaking knowledge of how it was built?
- Are diagnostic/validation concerns separated from business logic?

Language: analysis report in Russian, code references in English.
