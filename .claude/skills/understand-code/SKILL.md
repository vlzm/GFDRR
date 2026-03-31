---
name: understanding-notebook
description: >
  Generate a "Understanding Notebook" — a Jupyter notebook that helps the developer
  build a mental model of recently implemented code. Use this skill whenever the user
  asks to "create an understanding notebook", "make a walkthrough notebook",
  "help me understand the code", "unroll the logic", "reverse engineer the implementation",
  "create a notebook so I can follow the logic", "show me how this works step by step",
  or any variation of wanting a sequential, flat, pedagogical notebook that traces
  the logic of a module they didn't write themselves. Also trigger when the user says
  "сделай ноутбук для понимания", "разверни логику", "покажи как это работает",
  "ноутбук-разбор", or similar Russian-language requests. This skill should be used
  AFTER code implementation is complete, not during design or planning phases.
---

# Understanding Notebook Skill

## Purpose

The developer works with Claude Code to implement modules from design docs.
The code is well-structured (proper modules, classes, protocols) but the developer's
natural learning style is **bottom-up**: flat sequential logic, line by line,
no abstractions, building up to patterns. The generated code is **top-down**:
protocols, classes, modules — optimized for maintenance, not for first-time understanding.

This skill bridges that gap by generating a **Understanding Notebook** — a Jupyter
notebook the developer runs cell-by-cell to build their mental model.

## Architecture: Two Layers

### Layer 1 — Walkthrough (always generate)

Uses the **real classes and functions** from the implemented code, but calls them
sequentially with toy data, printing every intermediate result. Think of it as
a guided tour: the exhibits (code) are already in place, this is the route map.

Rules for Layer 1:
- Import real classes/functions from the actual module paths
- Create minimal toy data inline (or reuse test fixtures if they exist)
- One logical step per cell
- Every cell ends with `print()` or `display()` showing what just happened
- NO markdown-heavy explanations — short comments above code blocks only
- Show DataFrame shapes, column names, and first few rows at every step
- Follow the data flow order from the design doc, not the file structure
- Name pattern: `notebooks/understand/NN_module_name.ipynb`

### Layer 2 — Unfolded Logic (generate only for complex parts)

Takes the **internal logic** of complex classes/methods and rewrites it as flat,
sequential code — no classes, no method dispatch, no protocols. Just raw operations
on raw data, line by line. This is reverse engineering: showing what happens inside
the black box.

Rules for Layer 2:
- Appears as clearly marked sections WITHIN the same notebook (not a separate file)
- Header: `## Deep Dive: <ClassName.method_name>` or `## Deep Dive: <function_name>`
- Extract the actual implementation logic, flatten it into sequential operations
- Use the same toy data from Layer 1
- Show every intermediate variable with print/display
- After the unfolded section, add one cell that calls the real function and asserts
  the results match — proving the unfolded logic is faithful

### When to use Layer 2

Generate Layer 2 sections for code that meets ANY of these criteria:
- Contains non-trivial DataFrame operations (merges, pivots, groupbys, multi-step transforms)
- Implements a protocol/pattern where the dispatch logic is non-obvious
- Has conditional branching that affects data flow
- The developer explicitly asks to "unroll" or "unfold" specific parts

Do NOT generate Layer 2 for:
- Simple dataclass definitions
- Thin wrappers / delegation methods
- Config objects
- Straightforward CRUD-like operations

## Notebook Structure Template

```
# Understanding: <Module Name>
# Generated from: <design doc path>
# Code location: <module path>

## What this module does (2-3 sentences max, from design doc)

## Setup
  - imports, toy data creation

## Layer 1: Walkthrough
  ### Step 1: <first logical step from design doc>
    - cell: create input, call function/class, print result
  ### Step 2: <next step>
    - cell: ...
  ### ...
  ### Step N: Full Pipeline
    - cell: run everything end-to-end, show final state

## Layer 2: Deep Dives (only where needed)
  ### Deep Dive: <complex_function_name>
    - cell: flat sequential logic, no abstractions
    - cell: verify against real function
```

## Project-Specific Context

This skill is used in the GBP (Graph-Based Platform) project. Key conventions:

- **File locations:**
  - Understanding notebooks go in `notebooks/understand/`
  - Verification notebooks (different purpose!) go in `notebooks/verify/`
  - Design docs are in `docs/design/`
  - Storytelling docs are in `docs/story_telling/`

- **Existing patterns to follow:**
  - `notebooks/05_pipeline_walkthrough.ipynb` — good Layer 1 example (walkthrough of build pipeline)
  - `notebooks/verify/02_environment_skeleton.ipynb` — verification notebook (different purpose but similar cell style)

- **Data flow to respect:**
  ```
  Raw Data → RawModelData → build_model() → ResolvedModelData → Consumer
  ```
  Most understanding notebooks will start from `ResolvedModelData` (using test fixtures
  or `build_model(minimal_raw_model(...))`) and trace a consumer's logic.

- **Test fixtures:** reuse `tests/unit/build/fixtures.py::minimal_raw_model()` as the
  data source. Extend with `dataclasses.replace()` when the module needs additional
  tables populated.

- **Language:** All notebook content (markdown cells, comments, print messages) in English.
  Communication with the developer about the notebook — in Russian.

## Step-by-Step Process

When the user asks for an understanding notebook:

1. **Identify the target module.** Ask if not obvious. Check what was just implemented
   or what the user points to.

2. **Read the design doc** for that module (from `docs/design/`). This gives you the
   logical data flow order — which is the order for Layer 1.

3. **Read the implementation code.** Identify:
   - All public classes and functions
   - The data flow (what calls what, what data goes where)
   - Complex spots that need Layer 2 treatment

4. **Read existing test fixtures** to understand what toy data is available.

5. **Generate the notebook** following the two-layer structure above.

6. **Verify** the notebook runs without errors:
   ```bash
   cd <project_root>
   jupyter nbconvert --to notebook --execute notebooks/understand/<name>.ipynb \
     --output /tmp/test_output.ipynb 2>&1 | tail -5
   ```
   Fix any import errors or data issues.

7. **Report to the user** (in Russian) what the notebook covers and which parts
   got Layer 2 deep dives, so they know where to focus.

## Quality Checklist

Before delivering the notebook, verify:

- [ ] Every cell is self-contained (can be understood without reading other cells)
- [ ] Every cell produces visible output (print, display, or assertion)
- [ ] Layer 1 follows the design doc's logical flow, not the file structure
- [ ] Layer 2 sections have a verification cell at the end
- [ ] Toy data is minimal but sufficient (3-5 entities, 2-3 periods)
- [ ] No markdown walls — comments are brief, code speaks
- [ ] The notebook actually runs end-to-end without errors
- [ ] File is in `notebooks/understand/` with pattern `NN_module_name.ipynb`

## Anti-Patterns to Avoid

- **Don't recreate the design doc in markdown cells.** The notebook is code-first.
- **Don't import internal/private methods** unless they're the subject of a Deep Dive.
- **Don't add "exercise" cells** or TODOs for the developer. This isn't a tutorial.
- **Don't skip intermediate states.** Every transformation should show before/after.
- **Don't use `iterrows()` or loops** in Layer 2 unfolded code — use the same vectorized
  style as the real code, just without class/method wrapping.
- **Don't generate Layer 2 for everything.** Be selective. If the walkthrough makes
  it clear, Layer 2 is noise.
