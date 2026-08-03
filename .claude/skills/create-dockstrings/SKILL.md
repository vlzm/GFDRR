---
name: create-dockstrings
description: >
  Write or fix Python docstrings in this repo's minimal one-line style. Use this skill
  whenever the user asks to "write docstrings", "add docstrings", "convert docstrings",
  "fix docstrings", "shorten docstrings", "переделай докстринги", "напиши докстринги",
  "сократи докстринги", or to review or fix docstring style. Also trigger when adding
  docstrings to code that has none, or converting verbose (NumPy/Google-style) docstrings
  down to this repo's style. Applies to a single function and to whole-module sweeps.
---

# Minimal Docstring Standard

This repo keeps docstrings to a single summary line. Code should read as code,
not as prose. Type hints and the code itself carry the detail.

The line must also be easy to read. A reader who knows the codebase should
understand it on the first pass, without re-reading. When brevity and clarity
conflict, clarity wins: drop a detail rather than compress the sentence.

## Rules

1. One line per object. A docstring is a single summary sentence, ending with a
   period, on one physical line inside the triple quotes. No blank line, no body.

2. No sections. Never write Parameters, Returns, Raises, Notes, See Also,
   References, Examples, or an extended summary. Type hints in the signature are
   the source of truth for types; the code is the source of truth for behavior.

3. No external references. A docstring never points at a plan, a phase, a
   Notations.md section, or any other document. Each object describes itself in
   plain words.

4. Module docstrings are one line too — what the module builds or does, not an
   essay listing its functions or terminal commands.

5. One idea per docstring. Write what the object is or does, in plain English,
   imperative mood ("Build ...", "Return ...", "Round ..."). A second fact (an
   algorithm name, an invariant, a return-None case) may go in parentheses, but
   only if the line still reads easily on the first pass. If it does not fit
   readably, drop the fact or put it in an inline `#` comment — never cram it in.

6. Simple sentence shape:
   - One clause where possible. Never chain two facts with "so", "because",
     "which", or a second "and" — keep the main fact, move the other to an
     inline comment or drop it.
   - No reduced relative clauses and no clause-final prepositions. Not
     "the schemas the loader checked the tables against"; write
     "the loader's schemas" or "the schemas that the loader validated".
   - Name things explicitly. Every noun must be clear on its own: write
     "the original input tables", never "the originals"; "the engine's two
     input tables", never "two engine inputs".

7. Plain words. Short, common words over rare or figurative ones (avoid
   "dormant", "gating", "spine"). Keep field names, class names, and domain
   terms (OD matrix, stockout) unchanged.

8. Coverage. Public functions, methods, and classes get the one-line docstring.
   Private helpers (`_foo`) get one only when the name is not enough. Dunder
   methods other than `__init__` do not need one.

9. Implementation detail belongs in inline `#` comments, not in the docstring.
   Inline comments are never touched by this rule — leave them as they are.

10. The one line still obeys the repo's 100-character line limit. If it does not
    fit, drop the least important content — never save characters by deleting
    small connective words ("that", "the") or by swapping a clear phrase for a
    shorter, vaguer one. Never wrap the summary onto a second line.

## Examples

Module:

    """Build and save forecast demand tables."""

Class:

    class ForecastMeta(pydantic.BaseModel):
        """The meta.json contract of a forecast artifact."""

Function, plain:

    def load_forecast(name):
        """Read a saved forecast: the demand table and its validated meta.json."""

Function, one extra fact that still reads easily:

    def round_forecast_demand(demand_df):
        """Round a fractional forecast to whole bikes (largest-remainder, group total kept exact)."""

Function that can return None:

    def naive_month_prediction(month):
        """Forecast one month with the seasonal naive; None if no earlier partition."""

Bad — two facts chained, compressed references, clause-final preposition:

    """The sized tables replace two engine inputs, so they must fit the same schemas the loader checked the originals against."""

Good — one fact in the docstring, the other in an inline comment:

    def build_sized_tables(...):
        """Build the sized tables that replace the engine's two input tables."""
        # The sized tables must match the schemas that the loader validated
        # for the original input tables.

## Converting an existing verbose docstring

Keep the summary line and make it read simply (rules 5–7). Delete everything
else — every section, the extended summary, all cross-references and document
pointers. If the deleted body held a single fact a reader truly needs, fold it
into the summary line in parentheses only if the line stays easy to read;
otherwise move the fact to an inline `#` comment or drop it. Never expand a
one-line docstring into more lines.