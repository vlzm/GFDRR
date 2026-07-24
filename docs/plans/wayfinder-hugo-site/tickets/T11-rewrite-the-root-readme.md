---
id: T11
title: Rewrite the root README for the moved docs and the live site
label: wayfinder:task
status: closed
assignee: vlzm
blocked-by: []
---

## Question

The docs pages now live on the site (T10), so the root `README.md` is stale:
many of its links point into `docs/<section>/*.md` files that moved, and it
still shows `docs/assets/ui_overview.png`. Rewrite it as the repo's GitHub
front door — short, pointing at the published site
(https://vlzm.github.io/GFDRR/) rather than re-teaching the project.

For each link into a moved page, point at the site page (absolute site URL,
correct under the `/GFDRR/` base path) or at the in-repo private folder if it
stays out of the site. Repoint the screenshot: the image is already in the
site at `docs/site/assets/images/ui_overview.png` — decide whether the README
keeps a copy or links the site, then **delete `docs/assets/`** (T7: it goes
once the README stops referencing it).

Open sub-question to settle while doing it: how much of the current README
content stays in the README versus is now the site's job (the landing page,
T3, already carries the portfolio framing).

## Resolution

Rewrote `README.md` as a thin GitHub front door (~60 lines, down from ~238).
Sub-question settled: nothing teaching stays — the site owns it. The README now
keeps only a one-paragraph pitch, a prominent link to the live site, the
screenshot, a four-bullet "what is interesting", a minimal local-run block
(install extras + `streamlit` + the two canonical notebooks), the dev commands,
and two pointers (the site, and `Notations.md` at the repo root).

Stale `docs/<section>/*.md` links dropped rather than repointed one-by-one: the
thin README carries only the ones still needed — `Getting started` as an
absolute site URL (`.../docs/getting-started/`), the live-site root, and
`Notations.md` as a repo link (it stays at the root, T4). The former deep links
(api, scenario, quickstart) are reached through the site.

Screenshot: points at the in-repo site copy
`docs/site/assets/images/ui_overview.png` — one copy, renders on GitHub, no
dependency on the deployed site. `docs/assets/` deleted (`git rm`), closing the
T7 coupling that held it open.

Remaining `docs/` leftovers (`README.md`, `README_ru.md`, `archive/`) are
T12's, not this ticket's.
