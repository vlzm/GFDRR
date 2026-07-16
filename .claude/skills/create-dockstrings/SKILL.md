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

5. What to write: what the object is or does, in plain English, imperative mood
   ("Build ...", "Return ...", "Round ..."). If one non-obvious fact matters
   (an algorithm name, an invariant, a return-None case), fold it into the same
   line in parentheses — do not add a second line.

6. Coverage. Public functions, methods, and classes get the one-line docstring.
   Private helpers (`_foo`) get one only when the name is not enough. Dunder
   methods other than `__init__` do not need one.

7. Implementation detail belongs in inline `#` comments, not in the docstring.
   Inline comments are never touched by this rule — leave them as they are.

8. The one line still obeys the repo's 100-character line limit. If it does not
   fit, tighten the wording — never wrap the summary onto a second line.

## Examples

Module:

    """Build and save forecast demand tables."""

Class:

    class ForecastMeta(pydantic.BaseModel):
        """The meta.json contract of a forecast artifact."""

Function, plain:

    def load_forecast(name):
        """Read a saved forecast: the demand table and its validated meta.json."""

Function, one non-obvious fact folded in:

    def round_forecast_demand(demand_df):
        """Round a fractional forecast to whole bikes (largest-remainder, group total kept exact)."""

Function that can return None:

    def naive_month_prediction(month):
        """Forecast one month with the seasonal naive; None if no earlier partition."""

## Converting an existing verbose docstring

Keep the summary line. Delete everything else — every section, the extended
summary, all cross-references and document pointers. If the deleted body held a
single fact a reader truly needs, fold it into the summary line in parentheses;
otherwise drop it. Never expand a one-line docstring into more lines.
