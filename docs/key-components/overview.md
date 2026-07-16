# About the whole framework

This is the page to read first, and it is the only one meant to be read away
from the code. The other pages in this folder each stand around one part — the
loaders, the journal library, the simulator, rebalancing, the forecast toolkit,
the app. This page stands around all of them at once. Its job is to say what
the whole thing is really about, so that when you open any single page you
already know where it sits and what it is serving.

There is a temptation, with a system this size, to describe it as a list of
folders. That would be a map, not an explanation, and a map tells you where
things are without telling you why they are there. So this page takes a
position instead: the framework is not a collection of parts that happen to
work together. It is one idea, followed to its consequences, and almost every
design choice below falls out of that one idea once you see it.

## About the one idea: a run is a journal, not a state

Start with the thing everything else rests on. A run of this system does not
keep a running picture of the world that it edits as time passes. It keeps a
book. Every time a bike moves — a user takes it out, it docks, it bounces off a
full station, it is lost, a truck carries it at night — one line is written in
that book and never changed again. The book is the **flow journal**: an
append-only table where one row is one flow event. Bikes docked at stations,
demand, the origin-destination matrix, costs, the maps on screen — none of them
is stored. Each is *computed* from the journal whenever it is needed.

The plain version of the idea is that simple: record what happened, derive
everything else. The reason it matters is worth dwelling on, because it is the
whole bet. If you keep an editable state table and also keep summaries of it,
the two can drift — a bug updates one and forgets the other, and now your
inventory count and your list of trips quietly disagree, with no way to tell
which is lying. If instead there is a single record and every summary is a pure
function of it, they *cannot* disagree, because they are all counted from the
same rows. The system trades away the speed of a mutable running total for one
thing: that you can always ask "what happened in this run?" and get a truthful
answer, re-derived from the record, every time. The reasoning is set out in the
decision record
[journal-as-source-of-truth.md](../decisions/journal-as-source-of-truth.md),
and the journal library that owns this table — its schema, its builders, and
the read-only functions that turn it back into tables — is
[flow-journal.md](flow-journal.md). If you want to see the idea made concrete
before reading any design prose, [worked-examples.md](worked-examples.md) shows
the smallest real journals, one bike at a time, each one re-run by a test so it
cannot go stale.

This choice has a price, and it is honest to name it. The journal's schema
becomes the load-bearing wall of the whole project: change those columns and
every other part feels it. And "compute everything on demand" only stays
affordable because a run here is one month of one city — big, but not so big
that re-deriving a table hurts. On a hundred cities the arithmetic would change,
and the bet might not hold. It holds here on purpose, for a scale that was
chosen with it in mind.

## About the five words the whole system speaks

The second thing to know is that the code, below the entrance, does not talk
about bikes and stations. It talks about a flow network in general, in five
words. Learn them as one sentence and most of the codebase reads more easily.

A **commodity** is the thing that moves (here, a kind of bike). A **facility**
is a node it moves between (a station or a depot). An **edge** is the pair of
facilities a movement travels along, carrying a distance and a travel time. A
**resource** is the carrier that does the moving (a truck). And a **flow** is
one such movement once it is written down in the journal. Said in one breath: a
*flow* carries a *commodity* between *facilities* along an *edge*, sometimes on
a *resource*. Every one of these words is fixed in [Notations.md](../../Notations.md),
the project's dictionary, where one concept has exactly one word — before
anything is named in code, it is named there first.

Why strip the domain away at all? Because the simulator downstream should be
able to reason about flows on a graph without knowing they are bikes. The cost
is a layer of translation at the front door — a steady hum of nearly identical
functions that rename `station_id` to `facility_id`, `truck_id` to
`resource_id`, and so on. This is a real wager, and it is fair to disagree with
it: the abstraction earns its keep only if a second scenario ever arrives to use
it, and today there is exactly one. The project keeps the layer on purpose — see
the "vertical, not horizontal" rule in `CLAUDE.md` — but as a bet on a future
that has not come, not as an obviously free good. Where that translation happens,
and what it buys and costs, is the subject of [data-model.md](data-model.md).

## About the shape of one run

With those two ideas in hand, the shape of a run is almost a straight line. Raw
trip files come in; the loaders turn them into the input tables; the simulator
replays them and writes a journal; the artifact builder freezes that journal
into a folder of tables; the app reads the folder and draws it.

```text
raw trip CSVs
  -> loaders (gbp/loaders/)              build the input tables      [data-model.md]
  -> simulator (gbp/consumers/simulator) replay, writing the journal [simulation-engine.md]
  -> artifact builder (gbp/artifacts.py) freeze the journal to tables [visualization.md]
  -> run artifact (data/runs/<name>/)    one folder per run
  -> app (app/) or API                   read the folder and draw it [visualization.md]
```

Two parts sit just off this line. Truck **rebalancing** is an optional set of
phases inside the simulator — the night-time moving of bikes so more morning
trips can depart; it is [rebalancing.md](rebalancing.md). And **demand
forecasting** stands beside the whole line rather than inside it: a model
trained on past months writes a forecast demand table, and a run on that table
is the *same* straight line with a single substitution — the forecast takes the
place of history and nothing else changes. That is [ml-toolkit.md](ml-toolkit.md),
and the reason it is a substitution rather than a second engine is
[forecast-replaces-only-demand.md](../decisions/forecast-replaces-only-demand.md).

The substitution is the quiet key to the current phase of the project. The
canonical scenario is two runs: the base replay of one real month, and the same
machinery run on forecast demand. Because the two differ only in the demand
table they face, any difference in their results is caused by the forecast and
by nothing else. A separate "forecast simulator" was the tempting alternative
and was refused for exactly this reason — a second code path would have quietly
broken the comparison, and the comparison is the whole point.

## About why the parts are few and deep

One more choice shapes how the code is written, and it explains why this folder
has seven pages and not seventy. The project follows a single rule from John
Ousterhout's *A Philosophy of Software Design*: a module earns its place when
its interface is small and the work hidden behind it is large. The loaders hand
back one object and hide behind it the CSV cleaning, the period grid, the
origin-destination matrix, routing, and the sizing helpers. The simulator takes
one scenario and returns one journal. The journal library answers "what was the
inventory?" with one function whether the journal came from history or from a
run.

This is a deliberate trade, and it cuts both ways. A deep module is pleasant
from the outside and harder on the inside: a change often means opening one
large module rather than rewiring many small ones. The opposite style — many
tiny classes, each readable on its own — reads well one file at a time but makes
whoever uses it hold ten of them in mind at once. The project takes the first
trade on purpose: it would rather protect the caller from complexity than spread
that complexity thin. Each per-component page in this folder is, in effect, a
tour of the inside of one deep module — the part the small interface is hiding.

## What the whole design is betting on

Drawn into one thought, the framework is three bets stacked on each other. Keep
one honest, append-only journal, and compute every view from it. Keep the scope
vertical — one real scenario, known to every module by name — instead of a
general platform guessed at in advance. Keep the modules few and deep, paying
with harder insides for simpler outsides.

Each of these trades away something — the speed of a mutable state, the
generality of a domain-independent core, the ease of many small classes — for a
single payoff: that you can always ask what happened in a run and get a truthful
answer, re-derived from the record, at a scale where re-deriving it costs
little. Everything else in this folder is the detail of how that payoff is
earned. And the phase the project is now in — running the simulator on forecast
demand, side by side with the replay of history — is precisely the kind of
question that is only worth asking if the answer can be trusted. The whole
design exists to make that trust cheap.
