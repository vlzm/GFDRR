---
name: write-explanation
description: >
  Write or rewrite an "explanation" document in the Diátaxis sense — reflective prose
  that gives the bigger picture, the history, the "why", and a point of view — rather
  than a reference map or a how-to. Use this skill whenever the user asks to "write an
  explanation", "make this an explanation doc", "explain the design/architecture", "write
  the 'About X' doc", "turn this reference into an explanation", or asks for a reflective
  walkthrough of why something is built the way it is. Also trigger for Russian phrases:
  "напиши объяснение", "сделай объяснительный документ", "объясни устройство/архитектуру",
  "перепиши как объяснение", "разбор почему так сделано", "документ типа explanation".
  Applies to a whole .md document and to an explanation-style chat answer.
---

# Writing an Explanation

An explanation is one of the four Diátaxis document types (tutorial, how-to,
reference, explanation). Its job is understanding, not action. It stands
*around* a topic and answers "tell me about…?": the bigger picture, the
history, the choices and their alternatives, and the reasons behind them.

Read `references/explanation-diataxis.md` in this skill folder first — it is the
full Diátaxis guidance on what an explanation is and how to write one. This
SKILL.md only says how that genre changes the repo's default writing rules.

## Why this skill exists

The repo's default rules in `CLAUDE.md` (the "Answer Style" and "Plain
language" sections) are tuned for reference and how-to writing. They keep prose
terse, factual, and opinion-free. That is right for most docs — but it actively
suppresses the four things an explanation needs. When the user asks for an
explanation, follow the overrides below **for that document (or that chat
answer) only**. Everything not listed here stays as `CLAUDE.md` says.

## Rules this skill relaxes (explanation only)

1. **Open with framing, not a one-line answer.** `CLAUDE.md` says answer the
   specific question in one-two sentences with no roadmap. An explanation may
   instead open by setting up the topic — what this is really about, why it is
   worth understanding — before any detail.

2. **Give background the user did not literally ask for.** `CLAUDE.md` says do
   not explain things the user did not ask about. An explanation is expected to
   supply history, context, and connections to nearby topics — even ones
   outside the immediate question — when they help understanding.

3. **Offer judgment and weigh alternatives.** `CLAUDE.md` forbids grading the
   material ("no 'ключевая модель', 'самое тонкое место'") and opinion. An
   explanation must do the opposite: name the trade-off, say what a design buys
   and what it costs, consider the alternative that was not taken, and take a
   point of view. This is the heart of the genre — do not skip it.

4. **Metaphor and analogy are allowed when they clarify.** `CLAUDE.md` bans
   figurative words for technical things. An explanation may use an analogy or
   an image where it genuinely helps a reader hold the idea (Diátaxis itself
   leans on the "On Food and Cooking" analogy). The limit: the metaphor must
   sit next to the plain definition, never replace it. Define the thing plainly
   first, then illuminate it.

5. **Prose first, code as illustration.** `CLAUDE.md` says code first, words
   after, and favors `file:line` density. In an explanation the prose is the
   spine; a quoted code fragment appears to *illustrate* a point already being
   made, not as the thing being narrated line by line. Quote less code, and
   only the few lines that carry the idea.

6. **A synthesizing close is allowed.** `CLAUDE.md` forbids a closing summary.
   An explanation may end by drawing the threads into one thought — what the
   whole design is really betting on — as long as it adds a synthesis, not a
   restatement.

## Rules that still hold (never relaxed)

- **English only** for the document, same as code. (Russian stays for chat with
  the user.)
- **Plain language.** Short sentences, common words, one idea per sentence.
  Define every concept in plain words the first time it appears, before using
  it. Metaphor is a supplement to that definition, not a substitute.
- **Canonical vocabulary.** Code identifiers, file names, function names, and
  domain terms from `Notations.md` stay exactly as they are — never translate,
  rename, or rephrase them. Explanation freedom is about the surrounding prose,
  not about renaming the things.
- **Stay in your lane.** An explanation must not absorb how-to steps or a full
  reference field-list. When the reader needs those, link to the how-to or
  reference doc instead of pulling their content in.

## The test for a finished explanation

Before every heading you should be able to place the word "About" and have it
read naturally ("About the data model", "About why the state is computed"). If
a section reads as "Steps to…" or "Fields of…", it belongs in a how-to or a
reference, not here. And ask: does this doc take a position a reader could
disagree with? If it states only facts with no judgment, it is still a
reference wearing an explanation's title.
