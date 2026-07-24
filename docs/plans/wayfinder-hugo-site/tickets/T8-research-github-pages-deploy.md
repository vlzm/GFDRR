---
id: T8
title: Research the GitHub Pages deploy
label: wayfinder:research
status: closed
assignee: research-agent (charting session 2026-07-24)
closed: 2026-07-24
blocked-by: []
---

## Question

What is the current recommended way to publish a Hugo (hugo-book,
extended edition not required?) site from a subdirectory
(`docs/site/`) to GitHub Pages with GitHub Actions: the official
workflow file, the Pages settings ("GitHub Actions" source), the Hugo
version to pin, and how `baseURL` interacts with a project page
(`vlzm.github.io/GFDRR/`). Also note anything the repository name
implies: the site URL will contain `GFDRR` — flag it so the human can
decide if that is acceptable for a portfolio link. Findings go to
`research/github-pages-deploy.md` next to this map.

## Resolution (2026-07-24)

Findings: [research/github-pages-deploy.md](../research/github-pages-deploy.md).
The official Hugo workflow (Pages source = "GitHub Actions", no
`gh-pages` branch) works from `docs/site/` with two changes: `--source
docs/site` on the build and artifact path `docs/site/public`. One
correction to the official file is mandatory: hugo-book requires the
Hugo **extended** edition (v0.158+), so the workflow must download the
extended tarball; pin v0.164.0. `baseURL` is injected by
`actions/configure-pages` automatically, so a repo rename would not
break CI — but the Pages URL itself is NOT redirected on rename: decide
on the "GFDRR" name before sharing the link. Full adapted YAML is in
the findings file; the setup itself is ticket T9.
