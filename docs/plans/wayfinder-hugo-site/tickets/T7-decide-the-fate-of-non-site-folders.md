---
id: T7
title: Decide the fate of the non-site folders
label: wayfinder:grilling
status: closed
assignee: opus (work session 2026-07-24)
closed: 2026-07-24
blocked-by: []
---

## Question

The site takes over `docs/` as the home of the documentation. What
happens to everything that does not become a site page: `interview/`
(portfolio prep — feeds the landing page or stays private?),
`archive/`, `method/` (author's working notes, has Russian
companions), `assets/`, the Russian companion files (`README_ru.md`,
`*_ru.md`, `*.ru.md`) given the site is English-only, and `plans/` /
`reports/` (agreed to stay — but where: `docs/plans/` next to
`docs/site/`, or elsewhere)? The answer is a short table: folder →
stays at path / moves to X / deleted.

## Resolution (2026-07-24)

Fate table. The Diátaxis folders are listed only for completeness — moving
them is T10's job, not this ticket.

| Path | Fate |
|---|---|
| `docs/{getting-started,key-components,how-to,reference,scenarios,decisions}/` | Move into `docs/site/content/` — decided by T10, not here |
| `docs/README.md` | **Delete.** The site's landing page and section `_index` pages replace this hand-written index of `docs/`. |
| `docs/README_ru.md` | **Delete.** Its English partner is deleted. |
| `docs/archive/` (`overview.md`) | **Delete.** Superseded overview. |
| `docs/assets/` (`ui_overview.png`) | **Delete**, gated on the root `README.md` rewrite. The image is already byte-identical inside `docs/site/static/images/`; the root README (line 180) is the only remaining reference. The root README rewrite (Not-yet-specified) repoints it, then `docs/assets/` goes. |
| `docs/interview/` (`presentation_45min.md` + `_ru`) | **Stays** at `docs/interview/`, private — not published. Speaker notes / prep, not a reader-facing page. Its `_ru` companion stays too. |
| `docs/method/` | **Stays** at `docs/method/`, including `comprehension_levels_ru.md` and `working-method.ru.md`. |
| `docs/plans/` | **Stays** at `docs/plans/`, including `docs_maro_diataxis_plan_ru.md`. |
| `docs/reports/` | **Stays** at `docs/reports/`, including `architecture_review_202607_ru.md`. |

Russian-companion policy (site is English-only): a `*_ru.md` / `*.ru.md`
file is deleted only when its English partner moves into the site or is
itself deleted; it is kept next to any English partner that stays outside
the site. Under this rule the only Russian file deleted is
`docs/README_ru.md`; every other `_ru`/`.ru` file sits beside a
stays-outside partner and is kept.

Stay-outside folders keep their current paths (`docs/plans/`,
`docs/reports/`, `docs/method/`, `docs/interview/`) — siblings of
`docs/site/`, so no link changes are needed for them.
