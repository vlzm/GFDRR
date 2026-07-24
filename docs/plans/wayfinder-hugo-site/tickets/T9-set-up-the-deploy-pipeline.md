---
id: T9
title: Set up the deploy pipeline
label: wayfinder:task
status: closed
assignee: opus (work session 2026-07-24)
closed: 2026-07-24
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

## Resolution (2026-07-24) — live

The site is live at **https://vlzm.github.io/GFDRR/** (HTTP 200, title
"Home • Flow-graph framework").

What actually shipped, differing from the original plan in one way — the
publish branch:

- The site publishes from **`city_bike_mvp_accounting`**, not `main`
  (user's decision: that is their working branch; `main` is ~169 commits
  behind and not the source of truth). `.github/workflows/hugo.yaml`
  triggers on push to `city_bike_mvp_accounting`.
- `.github/workflows/hugo.yaml` and the `[caches.images]` block in
  `docs/site/hugo.toml` follow research/github-pages-deploy.md exactly.
- Two settings changes the human made on GitHub: Pages Source =
  "GitHub Actions"; and the **github-pages environment** deployment-branch
  rule had to be widened to allow `city_bike_mvp_accounting` (its default
  allows only the default branch — the first deploy failed with "Branch
  ... is not allowed to deploy to github-pages" until this was changed).
- Manual `workflow_dispatch` does not show a UI button because the deploy
  branch is not the repo's default branch; the push trigger is what runs.
