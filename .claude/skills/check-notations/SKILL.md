---
name: check-notations
description: >
  Audit the codebase against Notations.md, the project's canonical vocabulary,
  and report every place that names a known concept with a non-canonical word
  (drift) — with file:line and the canonical replacement. Use this skill
  whenever the user asks to check naming against the dictionary: "check
  notations", "audit the vocabulary", "check the canonical vocabulary", "is the
  code consistent with Notations.md", "find naming drift", "/check-notations".
  Also trigger for Russian phrases: "проверь словарь", "проверь нотации", "сверь
  код со словарём", "аудит словаря", "проверь соответствие словарю", "найди
  отклонения от канонического словаря", "проверь именование". This skill reads
  Notations.md as the source of truth, searches EXISTING code (it does not
  rename by default), and produces a drift report plus dictionary-hygiene notes.
---

# Check Notations Skill

## Purpose

Keep one concept = one word. `Notations.md` is the project's dictionary; this
skill finds where the code (and docstrings) drifted from it and reports each
spot with its canonical replacement. The output is a report, not edits —
renames, especially column renames across files, need a human decision.

`Notations.md` is the authority on *what* is canonical. This skill adds the
*how to search* knowledge: which "avoid" words have legitimate other meanings,
so the audit does not drown in false positives.

## Workflow

1. **Read `Notations.md` first.** It is the source of truth and it changes. Take
   the term list from its **"Avoid" table columns** and its **"Known drift to
   fix"** section. If anything below disagrees with the current file, the file
   wins — the table here is a convenience mirror, not the authority.
2. **Set the scope.** Default: `gbp/` and `tests/`. If the user names a file or
   directory, search only that. Notebooks are JSON and noisy — skip unless asked.
3. **Search each avoid-term** with the recipes below (word-boundary vs substring,
   and the false-positive drops). Use `rg -n`.
4. **Classify every hit** by reading its line and enough context:
   - *confident drift* — the avoid word names the governed concept;
   - *needs review* — the word also has a legitimate other sense here;
   - *drop* — a false positive (a canonical compound, an allowed context).
5. **Reverse pass (best-effort).** Scan for domain nouns that recur in the code
   but are absent from `Notations.md` — candidates to add to the dictionary.
   Keep this light; only flag obvious, repeated concepts.
6. **Produce the report** in the format below.
7. **Offer targeted fixes.** Do not rename by default. Offer to apply one concept
   at a time, and warn that column renames (e.g. `realized` → `departed`) touch
   `mechanics.py`, `state.py`, and `phases.py` together.

## Term list and false-positive rules

Mirror of the current `Notations.md` avoid-words, plus the context rules that
keep the search honest. Re-derive the words from the file each run; keep the
rules.

| Avoid | Canonical | Search | Context rule (false positives to drop / judge) |
|---|---|---|---|
| `stock`, `stock_before` | `inventory`, `inventory_before` | `\bstock` then drop `stockout` | `stockout` is **canonical** (a `lost` reason). Drop every `stockout`/`stockouts`. |
| `on-hand`, `on hand` | `inventory` | substring, case-insensitive | prose only. |
| `dispatched`, `released` | `departed` | substring | — |
| `placed`, `n_placed`, `placed_batches` | `redirected`, `n_redirected`, `redirected_batches` | substring `placed` | — |
| `rerouted` | `redirected` | substring | — |
| `shortfall` | `lost` **or** `leftover` | substring | **Two senses.** In `FormDeparturesPhase` (`phases.py`) = lost demand → `lost`/`lost_demand`. In `form_potential_trips` (`mechanics.py`, largest-remainder rounding) = the rounding leftover → `leftover`, a *different* concept. Judge by file/context. |
| `missing`, `dropped`, `failed` | `lost` | substring | High false-positive risk (`failed` in asserts/tests, `missing` in pandas calls). Flag **only** when used as a noun for lost demand/trips. Default to *needs review*. |
| `moving set`, `moving bikes` | `in_transit` / "in-transit" | `moving (set\|bike)` ci | prose. |
| `realized` (column / count / frame var) | `departed` (count), `departures` (frame) | substring | **Canonical and must be dropped:** `realized_target_id`, `realized_end_period` (the planned-vs-actual axis), and the verb `realize_departures`. Flag **only** a bare `realized` column, a frame variable named `realized`, or `realized` meaning the departure count. *Needs review.* |
| `origin`, `destination` | `source`, `target` | `\b(origin\|destination)s?\b` ci | **Allowed in the OD-matrix context** (Origin–Destination). Drop hits in `flows_to_od_matrix` and OD-matrix docstrings. The `origins` local in `_nearest_free_station` (`mechanics.py`) names *targets* — flag it. |
| `station` (as an identifier) | `facility` (`facility_id`) | substring `station` | Per `Notations.md` §4: `station` is **allowed in prose**, banned only as an identifier (`station_id`, `station = ...`, a `station` argument). Drop prose/docstring hits. Low priority. |

Rule of thumb for the search itself: rare distinctive words (`placed`,
`dispatched`, `shortfall`, `rerouted`) → plain substring and review every hit;
common stems (`stock`) → boundary search plus an explicit drop of the canonical
compound. `rg`'s default engine has no look-ahead, so prefer
`rg -n '\bstock' ... | rg -vi stockout` over a negative look-ahead.

## Report format

Prose in **Russian** (it is chat with the user). All identifiers, file paths,
and the words themselves stay in **English**, in backticks.

```
## Notations audit: <scope>

### Итог
- Найдено N мест: A уверенный дрейф, B под вопросом.
- Avoid-слова без единого совпадения (вероятно, уже починены): ...
- Концепты в коде, которых нет в словаре: ... (или «нет»)

### Уверенный дрейф
#### `stock` → `inventory`
- `gbp/consumers/simulator/phases.py:93` — `stock_before = ...` → `inventory_before`
- ...

### Под вопросом (зависит от контекста)
#### `shortfall`
- `gbp/.../mechanics.py:255` — остаток округления, НЕ потеря → `leftover` (не `lost`).
- `gbp/.../phases.py:169` — настоящий смысл «потеря» → `lost_demand`.

### Гигиена словаря
- Пункты «Known drift to fix» без совпадений (убрать из `Notations.md`): ...
- Кандидаты на добавление в словарь: ...
```

End with: report only, no renames done; offer to apply one concept at a time,
and flag that `realized` → `departed` is a multi-file column rename.

## Anti-patterns in the audit itself

- **Do not hardcode the vocabulary.** Always read `Notations.md` at run time. If
  the user edited it, the audit must follow — that is the whole point.
- **Do not flag canonical compounds.** `stockout` contains `stock` but is
  correct. `realized_target_id` contains `realized` but is correct. Dropping
  these is the difference between a useful report and noise.
- **Do not rename automatically.** Report first. Column renames cross files and
  need judgment per case.
- **Do not invent severities.** The axis here is confidence (confident drift vs
  needs review), not severity — every drift is equally worth one word.
- **Do close the loop on the dictionary.** When a "Known drift" entry has zero
  hits, say so and recommend removing it from `Notations.md`, so the file does
  not rot.
