---
name: update-docs
description: Review code changes from the current conversation and update project documentation (PROJECT_STATE.md, CLAUDE.md) if needed.
disable-model-invocation: true
---

Review all code changes made in this conversation. For each documentation file below, read its current state and decide whether it needs updating based on what changed.

## Files to check

### 1. PROJECT_STATE.md
Update if:
- Cleanup progress changed (modules removed, simplified)
- New issues discovered during cleanup
- The list of what canonical scenario uses changed

Do NOT update just because code was edited — only if the *project state* actually changed.

### 2. CLAUDE.md
Update if:
- New commands were added (build, test, lint)
- Architecture tree changed (packages removed or restructured)
- Code style rules changed

## Rules

- Read each file BEFORE modifying it.
- If nothing needs updating, say so explicitly — do not make cosmetic changes.
- Keep the existing style and structure of each file.
- English only in all documentation files.
- Be conservative: only update what the code changes actually require.
