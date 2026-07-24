---
id: T10
title: Move the docs pages into the site sections
label: wayfinder:task
status: closed
assignee: opus (work session 2026-07-24)
closed: 2026-07-24
blocked-by: [T2, T4, T6]
---

## Question

Move the in-scope `docs/` pages into the site sections fixed by T2 and
adapt each one. The folder-to-section map (from T2): `scenarios/` →
`scenario/`, `key-components/` → `architecture/`, `decisions/` →
`decisions/`, `getting-started/` → `getting-started/`, `how-to/` →
`how-to/`, `reference/` → `reference/`. For each moved page: add hugo
front matter (`title`, `weight` for order within its section, `math: true`
/ `mermaid: true` where needed), fix relative links (cross-section links,
images in `assets/`), and remove the page from the old `docs/` tree. The
Notations link target comes from T4; notebook links come from T6 — hence
the block on both. Leaves the landing page (T3) and the root `README.md`
rewrite (still fog) out of this ticket.

From T6: build the two notebook pages by running the committed
`nbconvert --to markdown` export script (Option 1 leaf bundles under
`content/docs/scenario/`), and repoint `scenarios/citibike.md` at those
pages instead of the raw `.ipynb`. Precondition: `forecast_pipeline.ipynb`
must be run once so it has saved outputs, otherwise its page is code-only.

## Resolution (2026-07-24)

All 21 in-scope pages moved into `docs/site/content/docs/<section>/`, each
with Hugo front matter (`title`, `weight`; `mermaid: true` on
`simulation-engine.md` and `worked-examples.md`). No page uses KaTeX
delimiters, so no `math` flag was needed. The old pages are removed from
`docs/` and the six empty source dirs are gone. `hugo v0.164.0+extended`
builds clean — no unresolved links, all Notations anchors resolve.

**Link strategy — `BookPortableLinks`.** Turned on `BookPortableLinks =
'warning'` in `hugo.toml` so the theme resolves relative `.md` links between
content pages. Every internal link stays a plain relative Markdown link:
- Cross-section and same-section page links: relative `.md` paths, folder
  renames applied (`../key-components/` → `../architecture/`,
  `../scenarios/` → `../scenario/`).
- Notations: `../../Notations.md#anchor` → `../reference/notations.md#anchor`
  (the T4 module mount; added the two mounts to `hugo.toml` for real, plus
  the `cascade: weight` for the mounted page in `reference/_index.md`).
- Image: `../assets/ui_overview.png` → `images/ui_overview.png`; the PNG
  moved from `static/images/` to `assets/images/` so its resource
  `RelPermalink` carries the `/GFDRR/` base path.
- Repo files that are not site pages (root `README.md`, `docs/reports/…`,
  `gbp/…`, `tests/…`): absolute GitHub URLs.
- The landing page's four `{{< relref >}}` links were converted to portable
  relative links — `relref` shortcodes conflict with `BookPortableLinks`.

**Notebook pages (T6).** Committed `scripts/export_notebooks.py` — runs
`nbconvert --to markdown` on the saved outputs (no execution), writes leaf
bundles `scenario/test-pipeline/` and `scenario/forecast-pipeline/`, and
prepends front matter. Ran it: `test-pipeline` has the six pandas tables and
the matplotlib PNG; `forecast-pipeline` is code-only (its notebook still has
no saved outputs — the T6 precondition, waiting on the user to run it once).
`citibike.md`'s notebook links now point at these pages. Added
`assets/_custom.scss` to let wide `dataframe` tables scroll sideways.

**Blocker found and fixed:** `.gitignore` line 74 was a bare `reference/`,
which matched *any* `reference/` dir — silently excluding
`docs/site/content/docs/reference/` (so the deployed site was already
missing its Reference section). The comment said the target was `.reference/`;
corrected the rule to `/.reference/`. Nothing tracked relied on the broad
form.

Changes are on disk and build clean but are **not yet committed**; a commit
+ push to `city_bike_mvp_accounting` triggers the deploy.
