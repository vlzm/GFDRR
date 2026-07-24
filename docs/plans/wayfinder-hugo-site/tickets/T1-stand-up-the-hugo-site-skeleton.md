---
id: T1
title: Stand up the Hugo site skeleton
label: wayfinder:task
status: closed
assignee: opus (work session 2026-07-24)
closed: 2026-07-24
blocked-by: []
---

## Question

Create `docs/site/` from the Pathsolver template so later structure
decisions can be seen live. Concretely: copy the hugo-book theme and
`hugo.toml` (adapt `baseURL`, `title`, `BookRepo`, `BookEditLink` to
this repository), drop everything Pathsolver-specific (`content/`,
`public/`, `resources/`, `static/images/fig.png`), keep the KaTeX /
Mermaid / search configuration, add a `.gitignore` for `public/` and
`resources/`, and confirm `hugo server` renders an empty site. Delete
`docs/template_hugo/` once harvested. No content moves yet — that is
later tickets' work.

## Resolution (2026-07-24)

Done. `docs/site/` now holds the empty skeleton and `hugo` builds it
clean (10 pages, 70 static files, no errors) with the locally-installed
`hugo v0.164.0+extended` — the exact version pinned in the deploy
research.

Harvested from the template into `docs/site/`:

- `themes/hugo-book/` — the whole vendored theme (KaTeX render hook,
  Mermaid, search), unchanged.
- `layouts/partials/docs/inject/head.html` — the one custom override,
  the KaTeX loader that fires on pages with `math: true`.
- `archetypes/default.md`, `.gitignore` (ignores `public/`,
  `resources/`, `.hugo_build.lock`).
- `hugo.toml` — the template's `[markup]` block kept verbatim (goldmark
  `unsafe`, KaTeX passthrough delimiters, ToC levels); only the identity
  fields changed: `baseURL = 'https://vlzm.github.io/GFDRR/'`,
  `BookRepo = 'https://github.com/vlzm/GFDRR'`, `BookEditLink` template
  repointed at `docs/site/`.

Created fresh (placeholders, replaced by later tickets): `content/_index.md`
(home) and `content/docs/_index.md` (the theme errors without a `docs`
section, so an empty site needs this one section page).

Dropped: all Pathsolver `content/`, `static/images/fig.png`, `public/`,
`resources/`. Empty `assets/`, `data/`, `i18n/` were not carried over —
nothing project-specific in them. `docs/template_hugo/` deleted.

Two notes for later tickets:

- `title = 'Flow-graph framework'` is a provisional working title; the
  real branding is the landing page's call (T3), tied to the repo-name
  decision flagged in the deploy research.
- The deploy research recommends a `[caches.images]` block in
  `hugo.toml` for the Actions image cache — left out here on purpose;
  it belongs to the deploy ticket (T9).

