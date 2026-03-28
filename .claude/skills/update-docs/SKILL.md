---
name: update-docs
description: Review code changes from the current conversation and update project documentation (PROJECT_STATE.md, CLAUDE.md) if needed.
disable-model-invocation: true
---

Review all code changes made in this conversation. For each documentation file below, read its current state and decide whether it needs updating based on what changed.

## Files to check

### 1. PROJECT_STATE.md
Update if:
- A roadmap item was completed or started
- New architecture decisions were made
- File map changed (new directories, renamed files)
- Current phase status changed

Do NOT update just because code was edited — only if the *project state* actually changed.

### 2. CLAUDE.md
Update if:
- New commands were added (build, test, lint)
- Architecture tree changed (new packages, directories, key files)
- New data model invariants were established
- Code style rules changed
- New key reference documents were created

### 3. docs/architecture_diagrams.md
Update if:
- New components or modules were added that should appear in diagrams
- Relationships between components changed

## Rules

- Read each file BEFORE modifying it.
- If nothing needs updating, say so explicitly — do not make cosmetic changes.
- Keep the existing style and structure of each file.
- English only in all documentation files.
- Be conservative: only update what the code changes actually require.
