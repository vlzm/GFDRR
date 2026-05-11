---
name: numpy-docstrings
description: >
  Write or convert docstrings to NumPy/SciPy style (numpydoc). Use this skill whenever the
  user asks to "write docstrings", "add docstrings", "convert docstrings", "переделай
  докстринги", "напиши докстринги", "numpydoc", "numpy style docstrings", "scipy style",
  or any variation of writing, rewriting, or converting Python docstrings. Also trigger
  when the user asks to review or fix docstring style and the target style is NumPy, or
  when docstrings need to be added to code that has none. If the user mentions "Google
  style" in the context of converting FROM it, this skill applies. Trigger for both
  single-function requests and batch "go through the whole module" requests.
---

# NumPy-Style Docstring Skill

## Core Reference

The authoritative format is numpydoc: https://numpydoc.readthedocs.io/en/latest/format.html

This skill encodes the full convention so you never need to fetch the reference.

## Cardinal Rules

These override everything else when in conflict.

1. Type hints in the signature are the source of truth. The docstring NEVER
   duplicates types that are already in the signature. If `def f(x: int) -> str`
   is the signature, the Parameters section shows the name and description only,
   no type. If there are NO type hints in the signature, the docstring MUST
   include the type on the name line.

2. Every public function, method, and class gets a docstring. Private methods
   (`_foo`) get a docstring only if the logic is non-obvious. Dunder methods
   other than `__init__` generally do not need docstrings.

3. The docstring describes WHAT and WHY, not HOW. Implementation details go
   in inline comments, not the docstring.

## Anatomy of a NumPy-Style Docstring

```python
def rebalance(
    graph: nx.DiGraph,
    capacity: int,
    *,
    timeout: float = 30.0,
) -> list[Route]:
    """Find minimum-cost routes that rebalance all stations.

    Uses a pickup-and-delivery formulation solved via OR-Tools CP-SAT.
    The solver treats each station deficit/surplus as a paired request
    and minimizes total travel time subject to vehicle capacity.

    Parameters
    ----------
    graph
        Station-level directed graph. Nodes must have ``demand`` (int)
        and ``coords`` (tuple[float, float]) attributes. Edges must
        have ``travel_time`` (float) weights.
    capacity
        Maximum number of bikes a single vehicle can carry at any
        point along a route.
    timeout
        Solver wall-clock limit in seconds. The solver returns the
        best feasible solution found within this budget; it does not
        guarantee optimality if time runs out.

    Returns
    -------
    list[Route]
        Ordered sequence of routes. Each ``Route`` contains the stop
        sequence and the load profile. Empty list if no feasible
        solution exists.

    Raises
    ------
    ValueError
        If any node is missing required attributes.
    InfeasibleError
        If the total surplus does not equal total deficit (i.e. the
        system is not closed).

    See Also
    --------
    build_station_graph : Constructs the input graph from trip data.
    Route : Dataclass describing a single vehicle route.

    Notes
    -----
    The formulation is a capacitated VRP with pickup-and-delivery
    constraints (CVRP-PD). Complexity is NP-hard in the general case;
    the CP-SAT solver uses branch-and-bound with constraint propagation.

    For the mathematical details, see [1]_.

    References
    ----------
    .. [1] Toth, P. and Vigo, D., "The Vehicle Routing Problem",
       SIAM, 2002.

    Examples
    --------
    >>> G = build_station_graph(trips_df)
    >>> routes = rebalance(G, capacity=20, timeout=10.0)
    >>> len(routes)
    3
    """
```

## Section Order

Always follow this order. Omit sections that do not apply — do not include
empty sections.

1. **Short summary** — one line, imperative mood ("Find ...", "Compute ...",
   "Return ..."), no variable names or type names. Terminated by a period.
   A blank line follows.

2. **Extended summary** — zero or more paragraphs elaborating on the short
   summary. Describes purpose, high-level algorithm choice, or important
   context the caller needs. Keep it concise — this is not a tutorial.

3. **Parameters** — every parameter of the function, in signature order.

4. **Returns** (or **Yields** for generators) — what comes back.

5. **Raises** — exceptions the caller should expect.

6. **Warns** — warnings issued (rare).

7. **See Also** — related functions/classes, one per line, with a short phrase.

8. **Notes** — mathematical background, algorithmic complexity, design
   rationale, anything that is important but secondary.

9. **References** — bibliography entries referenced from Notes.

10. **Examples** — short, runnable, doctest-compatible snippets.

## Formatting Rules

### Section headers

Section name followed by a line of dashes of equal length:

```
Parameters
----------
```

Not hyphens-minus-hyphens. Not underscores. Not equals signs.

### Parameter entries — WITH type hints in signature

When the signature already carries type annotations, the parameter entry
is just the bare name on one line and the description indented below:

```
Parameters
----------
graph
    Station-level directed graph with demand attributes on nodes.
capacity
    Maximum bikes per vehicle.
```

No type after the name. No colon after the name. The type hint in the
signature is sufficient; duplicating it violates DRY.

### Parameter entries — WITHOUT type hints in signature

When there are no type annotations (legacy code, C extensions, etc.),
the type goes on the name line after a space-colon-space:

```
Parameters
----------
graph : nx.DiGraph
    Station-level directed graph with demand attributes on nodes.
capacity : int
    Maximum bikes per vehicle.
```

### Optional parameters

Show the default value in the description, not on the name line:

```
timeout
    Solver wall-clock limit in seconds. Default is ``30.0``.
```

For parameters with `None` as default meaning "auto-detect" or "use a
sensible default", say what actually happens:

```
strategy
    Rebalancing strategy. If not provided, uses greedy nearest-neighbor.
```

### *args and **kwargs

```
*args
    Positional arguments forwarded to ``solver.solve()``.
**kwargs
    Keyword arguments forwarded to ``solver.solve()``.
```

If the forwarded arguments are well-known, list the important ones
explicitly and note that additional kwargs are passed through.

### Returns

If a single unnamed return value:

```
Returns
-------
list[Route]
    Ordered sequence of routes covering all imbalanced stations.
```

If returning a tuple or named tuple with multiple fields:

```
Returns
-------
routes : list[Route]
    The computed routes.
cost : float
    Total travel time across all routes.
```

Here names ARE included because the caller needs to know which position
is which. Types follow the same rule: include only if not inferrable
from the signature's return annotation.

### Yields (generators)

Same format as Returns but with the ``Yields`` header.

### Raises

```
Raises
------
ValueError
    If any node is missing required attributes.
```

Only list exceptions the caller should reasonably catch or expect.
Do not list every possible low-level exception (KeyError from a dict
access is an implementation detail, not a documented contract).

### See Also

```
See Also
--------
build_station_graph : Constructs the input graph from trip data.
Route : Dataclass describing a single vehicle route.
```

One entry per line. Name, space-colon-space, short description.

### Notes

Free-form paragraphs. Use RST math notation for formulas:

```
Notes
-----
The objective minimizes total travel time:

.. math:: \min \sum_{r \in R} \sum_{(i,j) \in r} t_{ij}

subject to capacity and pairing constraints.
```

### Examples

Must be valid doctest. Use ``>>>`` prompts. Keep examples minimal — they
demonstrate usage, not exhaustive testing.

```
Examples
--------
>>> G = build_station_graph(trips_df)
>>> routes = rebalance(G, capacity=20)
>>> assert all(r.is_feasible for r in routes)
```

## Class Docstrings

The class docstring goes directly under the class statement. Document
`__init__` parameters in the class docstring under a ``Parameters``
section, NOT in `__init__`'s own docstring. `__init__` either has no
docstring or a very brief one if it does non-trivial setup.

```python
class StationGraph:
    """Directed graph representing bike stations and travel links.

    Wraps a NetworkX DiGraph and adds domain-specific validation
    and accessors for demand/surplus computation.

    Parameters
    ----------
    raw_graph
        A NetworkX DiGraph with ``demand`` and ``coords`` node
        attributes and ``travel_time`` edge weights.
    name
        Human-readable identifier for logging. Default is ``"unnamed"``.

    Attributes
    ----------
    n_stations : int
        Number of stations (nodes) in the graph.
    total_demand : int
        Sum of all node demands (should be zero for a closed system).

    Examples
    --------
    >>> g = StationGraph(nx.DiGraph(), name="test")
    >>> g.n_stations
    0
    """
```

The ``Attributes`` section documents public instance attributes
(properties, descriptors, or plain attributes set in ``__init__``).
Format is the same as Parameters.

## Converting From Google Style

When converting existing Google-style docstrings, apply these
transformations:

1. ``Args:`` → ``Parameters`` with dashes underline.
2. Indented ``name (type): description`` → name-on-line, description
   indented below. Drop the type if the signature has type hints.
3. ``Returns:`` → ``Returns`` with dashes underline. Add the return
   type on its own line if not in signature.
4. ``Raises:`` → ``Raises`` with dashes underline. Exception class on
   its own line, description indented below.
5. ``Note:`` or ``Notes:`` → ``Notes`` with dashes underline.
6. ``Example:`` or ``Examples:`` → ``Examples`` with dashes underline.
   Ensure doctest ``>>>`` format.
7. Remove any inline type annotations from parameter descriptions
   if they duplicate the signature.
8. Ensure blank line between short summary and extended summary.
9. Ensure no blank line between the last section and the closing ``"""``.

## Practical Conventions

- Line length: wrap at 79 characters inside the docstring (72 is
  traditional but 79 is acceptable and matches PEP 8 code width).
- Use double backticks for inline code: ````capacity````.
- Use imperative mood for the short summary: "Compute X", not
  "Computes X" or "This function computes X".
- Boolean parameters: describe what ``True`` means. "If set, the solver
  logs progress to stderr."
- Do not start the description with "This is" or "A function that".
- Do not repeat the function name in the short summary.

## Batch Conversion Workflow

When asked to convert an entire module:

1. Read the module. Note which functions/classes have type hints and
   which do not — this determines whether types go in the docstring.
2. Convert one function and show the user for approval of the style.
3. After confirmation, convert the rest. Preserve any existing content
   (descriptions, examples, notes) — just reformat it.
4. If a function has no docstring at all, write one from scratch by
   reading the implementation.
5. Run a quick check: every Parameters entry matches an actual parameter
   in the signature (no stale params from refactoring).

## Anti-Patterns to Avoid

- Do not put a blank line between the closing line of a section and
  the closing ``"""``.
- Do not use Google-style ``Args:`` or reST-style ``:param x:``.
- Do not duplicate type hints in both the signature and the docstring.
- Do not write multi-line short summaries. If it does not fit on one
  line, move the extra content to the extended summary.
- Do not write "Parameters: None" or "Returns: None" — omit the
  section entirely.
- Do not use ``@param``, ``@type``, ``@return`` (Epydoc style).
- Do not describe implementation details ("loops over the list and...").
- Do not put ``Returns`` before ``Parameters``.
