---
id: T9
title: Set up the deploy pipeline
label: wayfinder:task
status: open
assignee: opus (work session 2026-07-24)
blocked-by: [T1, T8]
---

## Question

Following the researched recipe (ticket T8): add the GitHub Actions
workflow that builds `docs/site/` and publishes it to GitHub Pages,
set the Pages source in the repository settings (human step if the
agent cannot), push, and confirm the empty-skeleton site is reachable
at its public URL. Resolved when a live URL exists; record the URL in
the answer.

## Progress (2026-07-24, opus session) — not yet resolved

Done from the repo (follows research/github-pages-deploy.md exactly):

- Added `.github/workflows/hugo.yaml` — the official Hugo Pages workflow,
  adapted for `docs/site/` (extended edition, `--source docs/site`,
  artifact path `docs/site/public`, plus a `paths:` filter so unrelated
  commits don't rebuild Pages). Triggers on push to `main` and manual
  `workflow_dispatch`.
- Added the `[caches.images]` block to `docs/site/hugo.toml` (research §1).
- Verified `hugo build --source docs/site --gc --minify` runs clean on the
  local `v0.164.0+extended` — the exact CI build command.

Pending — needs the human (outward-facing, cannot be done from here):

1. **Enable Pages:** repo Settings → Pages → Build and deployment →
   Source: **GitHub Actions**. (Or `gh api -X POST repos/vlzm/GFDRR/pages
   -f build_type=workflow`.)
2. **Get this onto `main`:** the whole site lives on branch
   `city_bike_mvp_accounting`; the workflow only fires on `main`, so
   nothing publishes until the branch reaches `main`.
3. **Watch + confirm:** the run publishes to `https://vlzm.github.io/GFDRR/`.

Resolve (close) this ticket once that URL loads the skeleton site; record
the URL in the closing note.
