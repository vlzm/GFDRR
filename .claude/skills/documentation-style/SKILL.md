---
name: documentation-style
description: Write, rewrite, or review clear project documentation for docs Markdown files. Use when creating or editing docs/**/*.md, when the user asks to make documentation clearer or more structured, or when the user mentions documentation style.
---

# Documentation Style

Use this skill when writing, rewriting, or reviewing project documentation in
`docs/**/*.md`.

## First Steps

1. Read `.cursor/rules/documentation-style.mdc`.
2. Read `Notations.md` when the document names simulator concepts.
3. Read the current document, relevant source code, and nearby clear examples
   before writing.
4. Keep the edit scoped to documentation unless the user asks for code changes.

## Language

- Write project documents in plain English.
- Write a Russian companion only when the file name is `*_ru.md` or the user
  asks for a Russian document.
- Keep code identifiers, file names, column names, and function names unchanged.
- Use short sentences and common words.
- Do not use figurative wording for technical ideas.

## Vocabulary

- Use `Notations.md` as the project dictionary.
- Use one canonical word for one concept.
- Define a domain term before using it in a longer explanation.
- Do not invent near-synonyms for concepts that already exist in `Notations.md`.

## Structure

For simulator documents, prefer this order:

1. What the document explains and why the thing exists.
2. The small code map or data map.
3. The main state or tables carried through the process.
4. What runs first, second, third.
5. What each step reads, does, and writes.
6. What is checked.
7. Why the design works this way.

Put execution order before design reasons. A reader should first learn what
happens, then why it was built that way.

## Explanation Pattern

- Show real names first, then explain them.
- Use compact lists for ordered processes.
- Use small tables only when comparing a few concrete items.
- Avoid long paragraphs that mix state, mechanics, writes, and checks.
- Split those ideas into separate sections when the topic is complex.
- Link to `docs/scenarios.md` for row-by-row journal examples instead of
  repeating large scenario tables.

## Before Finishing

Check that a new reader can answer these questions:

- What runs?
- In what order does it run?
- What state or table changes?
- What is written to the journal or saved artifact?
- What proves the result stayed valid?

Run the editor diagnostics for changed files when available. If the document is
new, verify that its file name and language match the project convention.
