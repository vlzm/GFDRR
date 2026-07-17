# About the whole framework

This page is the one to read first, and the one to read away from the code. It
does not list files or tell you how to run anything — the other pages in this
folder do that, each for one part. Its job is to say what the whole thing *is*,
why it is shaped the way it is, and how the parts hold together, so that when
you open [data-model.md](data-model.md) or [simulation-engine.md](simulation-engine.md)
you already know where you are standing.

The project describes itself in one sentence: a framework for problems on flow
graphs — networks where commodities move between facilities. So far there is
exactly one such network in it, the Citi Bike bike-sharing system in New York.
That gap between the general sentence and the single concrete case is not an
accident to be tidied away; it is the central tension the whole design lives
inside, and much of what follows is about how the code holds both at once.

## About the two runs everything serves

Before any architecture, there is a purpose, and the purpose is small enough to
hold in one hand. The framework exists to produce two runs and let you compare
them.

The first is the base replay: take the real Citi Bike trips of some month, feed
them back through the simulator, and check that the model reproduces what
actually happened. The second is the forecast run: have a model predict future
demand, then run the same simulator on that prediction instead of on history.
The base replay is the honesty check — if the machine cannot reproduce the past,
its verdict on the future is worth nothing. The forecast run is the point of the
current phase of the work.

This is worth stating plainly because it is the rule that decides what belongs
in the codebase and what does not. If a piece of code does not serve one of
these two runs, it should be removed. That is a strong position, and it is a
deliberate defence against a real failure mode: a "flow graph framework" is the
kind of phrase that invites endless general machinery for scenarios that never
arrive. The two-runs rule is the brake on that. Everything below is here because
one of the two runs needs it.

## About the one bet the whole design rests on

If you understand only one idea about this project, make it this one: there is a
single source of truth, and it is a log of events called the flow journal. Every
number the system ever shows — inventory at a station, demand in an hour, the
map of trips, the cost of a run — is *computed* from that one table, never
stored beside it.

One row of the journal is one flow event: a bike departed, a bike arrived, a
bike bounced off a full station (`redirected`), or a bike was lost. Summaries
like demand or the origin-destination matrix are not separate facts; they are
the same journal viewed from one side. The code calls such a view a *marginal* —
the journal added up along one direction, ignoring the rest. The full design of
this layer is in [flow-journal.md](flow-journal.md), and the reasoning for
choosing it is written down as a decision in
[journal-as-source-of-truth.md](../decisions/journal-as-source-of-truth.md).

What does this bet buy? Consistency that cannot be broken by accident. Two views
computed from the same rows cannot disagree — the sum of departures always
matches the origin-destination matrix, because both are counted from the same
events. There is no reachable state where the numbers contradict each other,
because there is only one place the numbers come from.

What does it cost? The journal's schema becomes the load-bearing wall of the
whole project. Its columns are not a local detail of one module; they are the
contract every other part rests on. Change them and everything downstream feels
it. That is a real cost, and the project pays it on purpose: it would rather
have one thing that is hard to change than many things that quietly drift out of
agreement. Whether that trade is right depends on how stable the schema turns
out to be — so far it has held.

## About how one fact travels through the system

The parts of the framework are best understood as stages a fact passes through,
from a raw CSV row to a pixel on the screen. Read in that order they stop being
a pile of modules and become a pipeline.

**From trips to tables.** The loaders (`gbp/loaders/`) read the raw Citi Bike
trip files and build the tables the simulator reads. This is where the domain is
stripped off — more on that below — and where the historical journal is built
from real trips. The design and its trade-offs are in
[data-model.md](data-model.md).

**The shared vocabulary in the middle.** The model layer (`gbp/model/`) owns the
journal and the functions that read it. It imports neither the loaders nor the
simulator; both import *it*. That direction is deliberate and it is what makes
the two runs comparable: "historical inventory" and "simulated inventory" are
the same function called on two different journals, not two hand-written
definitions kept in sync. See [flow-journal.md](flow-journal.md).

**The run itself.** The simulator (`gbp/consumers/simulator/`) steps through the
periods one at a time. Each period it docks arriving bikes, forms this period's
departures from demand, docks the same-period arrivals, and — if enabled — moves
bikes by truck. It writes every movement as journal rows and, at the end, checks
that the finished journal and its own live state still agree. The map of phases
and the reasons behind the phase order are in
[simulation-engine.md](simulation-engine.md).

**The night truck layer.** Rebalancing is a self-contained part of the
simulator: it moves bikes by truck during a quiet night window so that more
morning trips can depart instead of failing on an empty station. It is optional
— a run without it is still a full run — which is why it reads best as its own
page, [rebalancing.md](rebalancing.md).

**From a finished run to the screen.** After a run finishes, its journal is
widened into a small folder of tables — the run artifact — computed once, at save
time. The Streamlit app is then a pure reader of those files: it loads tables,
slices them, and draws them, and it computes nothing the artifact builder could
have computed first. That discipline is what keeps every page fast and keeps two
runs saved months apart genuinely comparable. See
[visualization.md](visualization.md).

## About stripping the bikes out of the code

Here is a decision that surprises most readers of the code: almost nowhere does
the word "bike" or "station" appear. There is `commodity`, `facility`,
`resource`, `flow`. The loaders spend much of their effort renaming Citi Bike
columns into this neutral language, so that everything below them reasons about
flows on a graph rather than about bicycles.

This is the place where the general sentence at the top of the project meets the
single concrete scenario, and it is a genuine wager rather than an obviously
correct choice. The benefit is that the simulator, the journal, and the UI never
need to know they are about bikes; in principle a second scenario could reuse
them unchanged. The cost is a steady tax of nearly-identical rename functions and
a layer of indirection paid for a generality that, today, nobody spends — there
is still only one scenario. It is fair to hold an opinion here. The project keeps
the abstraction on purpose, but honestly: it is a bet on a future second domain,
not a free good. The full argument, with both sides, is in
[data-model.md](data-model.md).

## About time being counted, not dated

One more design choice runs through everything and is easy to miss: the system
has no continuous clock. Time is a grid of whole-numbered periods counted from
zero. Things do not happen at "14:37"; they happen in "period 342". Wall-clock
time is not thrown away — it lives as an attribute of each period — but it is no
longer the axis everything hangs on.

The reason is that the simulator is discrete: it advances one step at a time, and
a step has to be a countable thing. The payoff shows up most clearly in the
forecast path, where a second ruler over time appears — hour of week, a number
from 0 to 167 — precisely so that a future Wednesday at 14:00, which has no
history of its own, can borrow the routes of every past Wednesday at 14:00. That
move is what makes a forecast run possible at all. The details live in
[data-model.md](data-model.md).

## About the model that sits around the simulator

The forecasting work (`gbp/ml/`) is a ring of code around the simulator rather
than a change inside it. It turns trip history, weather, and station-status files
into a forecast demand table, and — this is the quiet elegance of it — that table
has exactly the same shape as historical demand. So the simulator does not know
whether it is running on the past or on a prediction; it just runs on "the demand
table it was given". A forecast run is a base replay with one table swapped, and
nothing else. The reasoning is written down in
[forecast-replaces-only-demand.md](../decisions/forecast-replaces-only-demand.md),
and the toolkit itself — the four model families, the backtest, the two-level
evaluation, the champion registry — is explained in
[ml-toolkit.md](ml-toolkit.md).

This shape is worth admiring for what it refuses to do. A more ambitious design
would fold the model into the simulator and let them influence each other. This
one keeps them apart on purpose: the forecast is a table, the simulator is a
consumer of tables, and the seam between them is the demand schema. That
narrowness is what lets the two-level evaluation be trustworthy — you can run the
simulator on real demand and on forecast demand with the same replay state, and
be sure any difference in the results is the forecast's doing and nothing else's.

## About how the design keeps itself honest

A system built on one derived source of truth has to prove that the source is
right, and this one does so in two ways that are worth seeing together.

The first is invariants. At the end of every run the simulator recomputes
inventory from the journal and compares it to the live inventory it carried
along; it checks that demand splits exactly into departures plus losses, that
every departed bike eventually docks or is lost, that no bike is created or
destroyed. These are not tests of one example — they are statements that must
hold for *any* run, checked on every run. They are listed in
[simulation-engine.md](simulation-engine.md).

The second is the worked examples. [worked-examples.md](worked-examples.md) is a
catalog of tiny scenarios — one bike, one truck route — each with its exact
journal rows printed in the page. The important part is that those rows are not
typed by hand: a test runs the real engine and compares its fresh journal to the
document cell by cell. If the code ever changes what the journal says, the test
fails before the document can go stale. It is a rare and admirable arrangement —
documentation that cannot lie, because a test forbids it.

Taken together these two mean the source of truth is not merely *declared* to be
consistent; it is checked to be, both in the general (invariants on every run)
and in the concrete (worked examples pinned to the code).

## What the whole thing is betting on

Pull the threads into one and the framework is a single wager made several times
over. It bets that a log of events is a better foundation than a pile of
summaries — and pays for it with a schema it must guard. It bets that stripping
the domain out buys a reuse it has not yet cashed in — and pays a tax of
translation for a second scenario that may or may not come. It bets that history
and forecast should run through one machine on interchangeable tables — and earns
an evaluation you can trust because of it. And it bets that documentation checked
by tests is worth the effort of writing scenarios that run.

To understand this codebase is not only to know where the files are; it is to see
these bets, and to have a view on whether each one is worth its price. The pages
this overview links to are where you go to form that view, one part at a time.
