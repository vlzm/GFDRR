# 05 — Update the canonical docs to the new gbp homes

**What to build:** the canonical text now names the moved homes. The paths
change; the rules do not. "artifact builder" stays the canonical word, now
living in `gbp`. Docs come last so they describe the finished code.

Changes (exact before → after in `docs/plans/app_to_gbp_seam.md`):

- **Notations §12** — every `app/artifacts.py` reference (the folder is "built
  by", "the contracts live in", "the pydantic model `RunMeta`", "the `METRICS`
  table in") becomes `gbp/artifacts.py`. No new table rows.
- **Notations §16** — "starts runs through the same `runner.run_scenario`"
  becomes "`run_scenario` (`gbp/consumers/run.py`)". The `API_URL` row (the
  `ui_shared.py` backend switch) is unchanged.
- **CLAUDE.md UI rule** — "the artifact builder (`app/artifacts.py`)" becomes
  "(`gbp/artifacts.py`)". The rule itself does not change.
- **CLAUDE.md Commands block** — `python app/evaluate.py --month 202601` becomes
  `python -m gbp.ml.evaluation --month 202601`. `python app/runner.py --help`
  is unchanged.

**Blocked by:** 01, 02, 03, 04 — docs describe the finished code.

**Status:** ready-for-agent

- [ ] Notations §12 names `gbp/artifacts.py` everywhere it named `app/artifacts.py`.
- [ ] Notations §16 names the `gbp/consumers` run path.
- [ ] The CLAUDE.md UI rule names `gbp/artifacts.py`; the rule wording is otherwise unchanged.
- [ ] The CLAUDE.md commands block shows `python -m gbp.ml.evaluation`.
- [ ] No stale `app/artifacts.py`, `app/evaluate.py`, or `runner.run_scenario` reference remains in Notations.md or CLAUDE.md (`grep` clean).
- [ ] `/check-notations` reports no drift introduced by the move.
