# 05 — Rewrite the canonical contract (Notations §12/§16, CLAUDE.md)

Type: grilling
Status: resolved
Blocked by: 01, 02, 03, 04

## Decision

Path references change, the rules do not. §12/§16 and the CLAUDE.md UI rule swap
`app/artifacts.py` → `gbp/artifacts.py` and the run-path module name.
"artifact builder" stays the canonical word, now living in gbp. No new §12
table rows (ticket 04 added none). Exact before→after wording is in
`docs/plans/app_to_gbp_seam.md`.

## Question

The moves decided in tickets 01–04 change text that is canonical. Decide the
exact new wording — this is a decision (naming + which sentences change), not
the edit itself.

What names the old boundary today and must be re-decided:
- **Notations.md §12** ("Run artifacts") says the run artifact folder is
  "built by `app/artifacts.py`" and "The contracts live in `app/artifacts.py`:
  `RunMeta` ..., `RUN_TABLE_SCHEMAS`, and `save_scenario_run`", and "The
  `METRICS` table in `app/artifacts.py`". Every one of these path references
  changes if the builder moves (ticket 01).
- **Notations.md §16** ("The run-artifact API") references the same layer.
- **CLAUDE.md** UI rule: "must not compute anything the artifact builder
  (`app/artifacts.py`) can precompute" — the path and possibly the rule shift
  with ticket 04.

Decisions to reach:
1. The new canonical word(s) for the moved layer — is "artifact builder" still
   the name, now living in gbp? Add/adjust the Notations entry (this is a
   `/domain-modeling` act: one concept, one word).
2. The exact §12 / §16 sentences that change, and their replacements.
3. The exact CLAUDE.md UI-rule sentence(s) that change.
4. Whether any new artifact tables from ticket 04 need a §12 row.

Consult: `/domain-modeling`, `check-notations`. Do not edit the docs — record
the decided wording so the execution effort can apply it verbatim.
