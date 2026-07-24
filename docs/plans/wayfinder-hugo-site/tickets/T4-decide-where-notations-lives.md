---
id: T4
title: Decide where Notations.md lives
label: wayfinder:grilling
status: closed
assignee: opus (work session 2026-07-24)
closed: 2026-07-24
blocked-by: []
---

## Question

`Notations.md` sits at the repository root and is the project's
dictionary; CLAUDE.md and several skills point at it by that path. The
site's reference section wants a glossary page. Options: move the file
into the site and update every pointer; keep it at the root and give
the site a page that links out to GitHub; keep it at the root and copy
it into the site at build time (two copies, one generated). Which one,
and who updates the pointers?

## Resolution

**Keep `Notations.md` at the repo root; mount it into the site.** The file
stays the one source of truth at the root, so no code pointer changes:
CLAUDE.md, the five skills (including `check-notations`, which reads it as
its source of truth), and every docstring citing it by path (`see
Notations.md §11`) keep working unchanged. Nobody updates pointers — that
is the point of this choice.

The reference section gets a real, searchable glossary page from a Hugo
module mount in `docs/site/hugo.toml`. Because defining any `content`
mount replaces the default, both mounts are declared:

```toml
[module]
  [[module.mounts]]
    source = "content"
    target = "content"
  [[module.mounts]]
    source = "../../Notations.md"
    target = "content/docs/reference/notations.md"
```

Verified on Hugo v0.164.0+extended: the build produces
`docs/reference/notations/index.html` as rendered HTML from the root file
(mounts from outside the project root are allowed). The config edit was
reverted after the test — the mount is T10's job to add for real.

**Notes for T10 (the page move):**
- `Notations.md` has no math delimiters, so no `math: true` is needed; the
  page title comes from its H1 (`# Notations — the canonical vocabulary`).
- The root file can carry no Hugo front matter (code and skills read it),
  so set the page's `weight` (order within the reference section) via
  `cascade` in `content/docs/reference/_index.md`, not on the file.
