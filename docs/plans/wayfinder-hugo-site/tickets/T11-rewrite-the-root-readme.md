---
id: T11
title: Rewrite the root README for the moved docs and the live site
label: wayfinder:task
status: open
assignee:
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
