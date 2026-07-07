# Working Method (personal cheat sheet)

> A reminder for how to work through a hard task without getting lost in my own head.
> Keep it short — a long version does not get read.

## One rule above all

**Every thought leaves a mark outside my head — or it did not happen.**

The head is great at the leap and the guess. It is bad at holding eight things at
once. For problems about data and state, paper *is* the thinking; the head is the
part that lies and says it already understood.

## The card (5 steps, in order)

1. **Goal** — one sentence: what is true when it is done. If I can't write it, I
   don't understand the task yet.
2. **One example** — the smallest real case, with real numbers. Not the general
   case. One.
3. **Run it by hand** — write the input and the output of that case. This is where
   I find what I actually need.
4. **Generalize — only now**, and only as far as a second example forces it.
   Generalize from two points, not from imagination.
5. **Polish — last**: names, format, optimization. Never first.

## Head, paper, code — when each

**Head** — for the single next move: what to do next, which guess to test, which
example to pick. Not for holding the whole task. Sign the head is enough: I can say
the whole answer out loud without pausing to rebuild it. The moment I catch "wait,
where was I" or re-derive something I already worked out — go to paper.

**Paper** — for understanding one concrete case: the input state, the output rows,
the invariants. Cheap to be wrong here and to find out what I actually need. Sign of
"won't fit in the head → paper": more than one thing changes over time; I need to
check several things against each other; a "what if" showed up.

**Code** — to make it real, runnable, and checkable, and to lock in what I
understood (so I never re-derive it). Sign of "paper → code": the by-hand trace has
settled and I trust it; or I am no longer sure the by-hand trace is even right (run
it and find out); or re-tracing by hand got tedious (let the machine trace it).

## The card runs per scenario

The card is one loop **per scenario**, not one pass for the whole project. Between
scenarios, code goes in the middle: the card (steps 1–3) gives the spec, then I make
exactly that one scenario work in code and check its output against the rows I wrote
by hand — the paper case becomes the test. Only then do I take the next, harder
scenario. "Generalize" (step 4) is a refactor of working code when a new case forces
it, not a paper design of everything up front.

## When I'm pulled into my head

- **"What if" is not an order to think the general case. It is a new example for
  the queue.** Keep two lists: left = "solving now" (one case), right = "what-if
  shelf". Every "what if" goes right as another concrete case for later. I stay on
  the left. Infinite branching becomes a finite list.
- **The pull to generalize is often an escape from a hard concrete detail.** When I
  feel it, ask: "which number am I avoiding writing down right now?" Write that one.
- **The instinct to generalize is good — only the timing is wrong.** Before
  examples it is guessing; after them it is abstraction. Same skill, later moment.
  Generalize from two worked examples, never from zero.
- **Paper is not a commitment.** Scribble, make it ugly, throw it away. The example
  is disposable; it does not have to be right or pretty.

To break the inertia at the start: before thinking anything, write the goal
sentence and one row of input data. The act of writing the first row usually breaks
the spell.
