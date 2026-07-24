---
label: wayfinder:map
title: Hugo documentation site
tickets: tickets/
---

# Wayfinder map — Hugo documentation site

## Destination

The documentation lives as a Hugo site (hugo-book theme) in `docs/site/`,
published on GitHub Pages. The site is the home of the current `docs/`
pages — the Markdown moves into `docs/site/content/`; only `plans/`,
`reports/`, `method/` (and whatever a ticket decides) stay outside. The
front page is written for a portfolio reader: it presents the project
before it teaches it. English only. This effort carries execution: the
map is done when the site is live and the old `docs/` tree is gone.

## Notes

- Settled while charting (2026-07-24): audience is portfolio /
  interviews; the site becomes the home of `docs/`; hosting is GitHub
  Pages; language is English only.
- The starting material: `docs/` is already organized by Diátaxis
  (getting-started, key-components, how-to, reference, scenarios,
  decisions). `docs/template_hugo/site/` is a working hugo-book site
  from the Pathsolver project — harvest its config (KaTeX, Mermaid,
  search, theme), then delete it.
- Tracker: local markdown. Tickets are files in `tickets/`, one per
  file. `status: open/closed` in the front matter; `assignee` is the
  claim; `blocked-by` lists ticket ids. The frontier = open, unblocked,
  unassigned tickets.
- Skills to consult when resolving tickets: `write-explanation` for any
  explanation-style page, `prototype` for structure/landing tickets.
  CLAUDE.md plain-language rules apply to every site page.

## Decisions so far

<!-- one line per closed ticket -->

- [Research notebook rendering options](tickets/T5-research-notebook-rendering-options.md)
  — five ways compared; researcher recommends nbconvert-to-markdown
  page bundles plus an embedded-HTML interactive copy; findings in
  [research/notebook-rendering.md](research/notebook-rendering.md).
- [Research the GitHub Pages deploy](tickets/T8-research-github-pages-deploy.md)
  — official Actions workflow works from `docs/site/` with `--source`
  and artifact-path changes; hugo-book needs Hugo **extended** (pin
  v0.164.0); Pages URL is not redirected if the repo is renamed;
  findings in
  [research/github-pages-deploy.md](research/github-pages-deploy.md).
- [Stand up the Hugo site skeleton](tickets/T1-stand-up-the-hugo-site-skeleton.md)
  — `docs/site/` now holds the empty hugo-book skeleton (theme, KaTeX
  override, adapted `hugo.toml` pointing at `vlzm/GFDRR`); `hugo` builds
  it clean with local `v0.164.0+extended`; `docs/template_hugo/`
  deleted. Unblocks the structure (T2) and landing (T3) tickets.
- [Choose the section structure](tickets/T2-choose-the-section-structure.md)
  — six sections under `content/docs/`, ordered for the portfolio reader:
  Scenario (1) → Architecture (2, was key-components) → Design decisions
  (3, own section) → Getting started (4) → How-to (5) → Reference (6).
  Built as live `_index.md` stubs. Unblocks the page move (T10).
- [Design the landing page](tickets/T3-design-the-landing-page.md)
  — `content/_index.md` built live: Streamlit screenshot as the hero image
  (copied to `static/images/`), a "By the numbers" line with the Jan 2026
  run figures, and four "where to go next" links by section (not the
  README's run/understand/change triple). Builds clean.
- [Decide where Notations.md lives](tickets/T4-decide-where-notations-lives.md)
  — stays at the repo root (one source of truth; no code pointer changes);
  the reference section renders it via a Hugo module mount of
  `../../Notations.md`, verified to build. Unblocks the page move (T10),
  which still waits on T6.
- [Decide how notebooks appear on the site](tickets/T6-decide-how-notebooks-appear-on-the-site.md)
  — Option 1 only: each notebook → a Markdown leaf bundle under
  `content/docs/scenario/` via a committed, hand-run `nbconvert --to
  markdown` script (no HTML copy, no execution in CI). Notebooks checked:
  `test_pipeline` has no plotly (the research's main worry is moot);
  `forecast_pipeline` has no saved outputs, so it must be run once before
  its page shows results. **Unblocks the page move (T10)** — now fully
  unblocked.
- [Set up the deploy pipeline](tickets/T9-set-up-the-deploy-pipeline.md)
  — the site is **live at https://vlzm.github.io/GFDRR/**. Publishes from
  branch `city_bike_mvp_accounting` (not `main`, which is ~169 commits
  behind) via `.github/workflows/hugo.yaml`, the researched Hugo→Pages
  workflow. Human steps done on GitHub: Pages Source = "GitHub Actions",
  and the github-pages environment widened to allow the deploy branch.
- [Decide the fate of the non-site folders](tickets/T7-decide-the-fate-of-non-site-folders.md)
  — fate table settled: delete `docs/README.md`, `docs/README_ru.md`,
  `docs/archive/`; `docs/assets/` deleted once the root README rewrite
  repoints its screenshot (image already in the site). `interview/`,
  `method/`, `plans/`, `reports/` stay at their current paths, private.
  Russian companions are deleted only when their English partner moves
  into the site — so only `docs/README_ru.md` goes.
- [Move the docs pages into the site sections](tickets/T10-move-the-docs-pages-into-the-site.md)
  — all 21 in-scope pages now live under `content/docs/<section>/` with
  Hugo front matter; old pages and empty source dirs removed; the site
  builds clean. Internal links kept as relative `.md` links via
  `BookPortableLinks`; Notations mounted with a `cascade` weight; the
  scenario page shows the two notebooks as `nbconvert` leaf bundles
  (`scripts/export_notebooks.py`; `forecast-pipeline` code-only until run).
  Fixed a `.gitignore` bug (`reference/` was hiding the site's Reference
  section). Graduates the README rewrite (T11) and the leftover deletions
  (T12).
- [Rewrite the root README](tickets/T11-rewrite-the-root-readme.md)
  — `README.md` is now a thin GitHub front door (~60 lines): pitch, a
  prominent link to the live site, the screenshot, four "what is
  interesting" bullets, a minimal local-run block, dev commands, and
  pointers. Stale `docs/<section>` deep links dropped (reached through the
  site); `Notations.md` kept as a repo link. Screenshot points at the
  in-repo site copy `docs/site/assets/images/ui_overview.png`; `docs/assets/`
  deleted, closing the T7 coupling. Leftover deletions remain T12.
- [Delete the retired docs/ leftovers](tickets/T12-delete-the-retired-docs-leftovers.md)
  — deleted `docs/README.md`, `docs/README_ru.md`, `docs/archive/` (only
  references were historical prose in private `docs/plans/*`). `docs/` now
  holds only `site/` and the private folders — **the destination is reached
  and the map is done.**

## Not yet specified

- Whether the site gets a dedicated "Results" showcase section (figures
  from `reports/`). The landing page (T3) already absorbs a UI screenshot
  and a one-line results block, so this is now only about a separate
  section, not about the front page.
- A visual check that Mermaid and search render on the moved pages once
  deployed. T10 confirmed the build is clean, set `mermaid: true` on the
  two Mermaid pages, and found no KaTeX math on any page, so this is now a
  small in-browser confirmation, not a build question.

<!-- Graduated from fog by T10: the README rewrite is now the live ticket
T11; the leftover `docs/` deletions are T12. -->


## Out of scope

- A bilingual site — settled while charting: English only. The Russian
  companion files' fate is decided (T7: kept beside stays-outside
  partners, deleted only when the English partner moves in), but a
  Russian *site* version is out of scope.
- Rewriting the documentation content beyond what the move and the
  portfolio front page require — the pages are already written.
- Hosting anywhere other than GitHub Pages.
