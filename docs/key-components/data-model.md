# About the data model

This page is an explanation, not a map. It stands around the data model that
`gbp/loaders/` builds — from the raw Citi Bike trip CSV to `ResolvedModelData`,
the tables the simulator reads — and tries to say *why* it is shaped the way it
is, what each decision buys, and what it costs. For the exact list of tables and
functions, read the class docstring and the code; for how the simulator uses
these tables, see [simulation-engine.md](simulation-engine.md); for the journal
functions, [flow-journal.md](flow-journal.md). This page is for reading away
from the code.

## About the one decision behind everything else

Step back far enough and the whole loader exists to serve a single sentence:
*commodities move between facilities*. In this project the commodities are
bikes and the facilities are Citi Bike stations. Yet almost nowhere in the code
does the word "bike" or "station" appear. Instead there is `commodity`,
`facility`, `resource`, `flow`. That is not a stylistic quirk — it is the
governing decision of the module, and it is worth understanding before anything
else.

There are two layers of data, and the loader is the translator between them.
The first is raw (`RawModelData`): stations, depots, trucks, trips — the tables
in the language of Citi Bike. The second is resolved (`ResolvedModelData`; read
*resolved* as "brought to a common form"): the same facts in the language of a
flow network in general — facilities, resources, commodities, flows.

Nearly every function at the top of `dataloader_graph.py` makes the same move:
take a table with a `station_id` or `truck_id` column, rename it to
`facility_id` or `resource_id`, and add a category. Stations and depots merge
into one kind of thing, a facility; trucks become resources; bike types become
commodity categories. The full rename mapping is the "raw → canonical boundary"
in [Notations.md §4](../../Notations.md#4-facility-and-its-roles-in-a-trip).

Why do it this way? Because the simulator downstream should not know it is about
bikes; it reasons about flows on a graph. The price of that is a steady hum of
almost-identical rename functions in the loader. The benefit is that everything
below the loader is free of the domain.

This is a bet, and it is fair to have an opinion about it. The translation layer
earns its keep only as long as there is genuinely more than one domain in view —
or a real intent to add one. If a second scenario never arrives, this layer is a
tax paid for a generality nobody spends. The project keeps it on purpose (see
the flow-graph sentence in `CLAUDE.md`), but it is a wager on the future, not a
free good. Worth naming plainly rather than treating the abstraction as
obviously correct.

## About why the journal is the source of truth

The most interesting thing in this model is a thing that is *not* an independent
table. Demand, arrivals, departures, the OD matrix (the table of how many trips
go from each facility to each other facility), inventory on hand — each looks
like its own table, and each has its own field on the object
(`historical_demand_df`, `historical_arrivals_df`, `historical_od_matrix_df`,
and so on). But none of them is a source. Every one is *computed* from a single
table: the flow journal, the event-by-event log of what moved
(`historical_flows_df`).

Watch how they come into being:

```python
self.historical_flows_df = get_historical_flows_df(raw.trips_df, self.t0, period_len)
...
self.historical_arrivals_df = flows_to_arrivals(self.historical_flows_df)
self.historical_od_matrix_df = flows_to_od_matrix(self.historical_flows_df)
```

The journal is built first. Then each summary table is a function of it. The
code calls these summaries *marginals* — a marginal is one view of the journal,
the journal summed up along one direction and ignoring the rest. Demand is the
journal seen from the side of departures. The OD matrix is the same journal
folded onto pairs of facilities. Inventory is the same journal added up over
time. Think of them as shadows the same object throws on different walls: the
object is the journal, and each shadow is a marginal.

This decision is worth admiring, and worth understanding the cost of. The reason
to admire it: one source and many views cannot disagree with each other, because
they are all computed from the same rows. There is no reachable state in which
the sum of departures fails to match the OD matrix — both are counted from the
same journal. This is exactly the property the decision record
[journal-as-source-of-truth.md](../decisions/journal-as-source-of-truth.md)
argues for.

The cost is that the journal becomes the load-bearing wall of the whole project.
Its schema — the nineteen columns of `FLOW_EVENT_COLUMNS` — is not a loader
detail; it is the contract everything else rests on. And it is a finer-grained
thing than a "trip": one event is a departure, an arrival, a redirect, or a
loss. A trip unfolds into a pair of events (`departed` on the way out, `arrived`
when the bike docks), and if the bike bounced off a full station along the way,
more events. Hence the two ids inside a trip — `move_id` for which leg of the
journey and `event_id` for which event. That pairing is quiet but load-bearing:
it is what lets the journal be both a flat list of facts and a story you can
replay bike by bike. The details are in [flow-journal.md](flow-journal.md).

## About why time is a grid, not a calendar

Another decision easy to miss: this model has no continuous time. It has a grid
of whole-numbered periods, counted from zero.

```python
def to_period_id(ts, t0, period_len):
    return ((ts - t0) // period_len).astype("int64")
```

Everything that happens is pinned not to "2026-01-15 14:37" but to "period 342".
`t0` is the origin (the first trip, floored to the hour); a period is one hour by
default. The `PeriodGrid` is the simulator's clock — it does not tick in real
calendar time, it counts steps.

Why counts and not dates? Because the simulator is discrete: it advances one step
at a time, and a step must be a countable thing, not an astronomical one. Whole
numbers are easier to compare, group, and use as an index. Wall-clock time is not
thrown away — it lives in the `start_timestamp` / `end_timestamp` of the periods
table — but it becomes an *attribute* of a period rather than the axis everything
hangs on.

It is worth seeing where this pays off. In the forecast path
(`map_od_matrix_by_hour_of_week`) a second notion of time appears: hour of week,
a number from 0 to 167. It exists to carry historical routes onto future
periods — a future Wednesday 14:00 has no history of its own, but every past
Wednesday 14:00 does. The period grid and the hour of week are two different
rulings laid over the same time, and the model moves data freely from one to the
other. That is the moment the "time as an index" abstraction stops being a
convenience and becomes the thing that makes a forecast run possible at all.

## About why history and simulation share one machine

Look at the fields of `ResolvedModelData` and they come in pairs.
`historical_flows_df` and `simulated_flows_df`. `historical_demand_df` and
`simulated_demand_df`. And so on down the list.

The historical ones fill at load time, from real trips. The simulated ones start
as `None` and fill later, from `attach_simulation`, once the simulator has run
and returned its own journal. What matters is that they fill through the *same*
functions:

```python
resolved.simulated_arrivals_df = flows_to_arrivals(resolved.simulated_flows_df)
resolved.simulated_od_matrix_df = flows_to_od_matrix(resolved.simulated_flows_df)
```

History and simulation go through one apparatus. The only difference between them
is where the journal came from: real trips, or the simulator. After that they are
indistinguishable.

This is what makes the base replay check honest by construction. Comparing "what
happened" to "what the model computed" is only fair if both sides were counted
the same way. Because both pass through identical read-models, any gap between
them is a gap in *behavior*, not an artifact of two different counting methods.
Had history been computed by one path and simulation by another, every
comparison would have to keep apologizing for the difference. Here it does not.

## About why everything is checked at one boundary

This model does not trust its inputs, and it says so out loud. At the end of
`__init__` every engine-facing table is checked against a schema, and the
historical journal is checked the moment it is built:

```python
violations = check_journal_schema(flows)
if violations:
    raise ValueError("historical flow journal breaks the journal schema:\n" + ...)
```

The code calls this the load boundary. The idea is simple: bad data should fail
here, at the entrance, not surface ten minutes into a run as an obscure pandas
error deep in the simulator. The check sits once, on a clearly drawn line, and
everything past that line is treated as correct in shape.

Next to it lives a check of a different kind — not "is each table's shape right"
but "do two tables agree with each other":

```python
assert init_by_cat.equals(sop0_by_cat), ...
```

It confirms that the initial inventory matches the start-of-period inventory at
period 0, computed from the journal. A single table's schema cannot express this
— it is a statement about the relation between two tables. The distinction is
worth holding onto: schemas guard the shape of each piece on its own, and the
assert guards the agreement of the whole. Both are about failing early, at
different levels.

## About why a forecast run is a copy with swapped demand

The last decision that reveals the model's character is how a forecast run is
set up (`apply_forecast_demand`). You might expect a forecast to build a fresh
object from scratch. Instead it makes a shallow copy of the existing one and
replaces only a few fields:

```python
out = copy.copy(resolved)
out.periods_df = forecast_periods_df
out.historical_demand_df = forecast_demand_df
out.historical_od_matrix_df = od_matrix_df
```

Geography, station capacities, speeds, routes — all the expensive, unchanging
part — is reused as is, shared between the base and forecast scenarios. Only what
truly separates the future from the past changes: how much is expected
(`historical_demand_df`) and along which routes it travels (`od_matrix_df`). The
same trick drives `apply_truck_fleet`: a copy with a new fleet. The reasoning is
laid out in
[forecast-replaces-only-demand.md](../decisions/forecast-replaces-only-demand.md).

There is a trade-off here worth stating. A shallow copy with shared tables is
fast and thrifty, but it rests on an unwritten agreement: a swapped table is
replaced whole (a new reference is assigned), never edited in place. Keep that
agreement and two scenarios share memory without disturbing each other. Break it
— edit a shared table in place — and the change silently leaks into the other
scenario. This buys speed at the price of discipline, and it holds exactly as far
as the author of the next `apply_*` function is disciplined.

Notice too that even in a forecast the field is still called
`historical_demand_df`, though what sits in it is a forecast. The name is
inherited from the base path. The simulator reads that field regardless of what
is really in it — to the engine it is just "the demand table to run on". The name
gives away the code's history rather than the data's current meaning — a small
thing, but an honest one: it shows that the forecast path grew out of the
historical one, rather than being designed apart from the start.

## What the whole thing is betting on

Drawn into one thought: this data model is built around a single journal of
events, from which everything else is derived; time in it is a counted grid, not
a calendar; the domain (bikes, stations) is stripped off at the entrance and
replaced by the language of flows on a graph; and history and forecast are one
machine run on different demand tables. Each of these is a wager with a clear
price — a translation tax for generality, a load-bearing schema for consistency,
shared memory for speed. To understand the model is to see not only how it is put
together, but what, exactly, it has bet on.
