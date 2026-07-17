# 45-minute presentation script

This is a speaking script, not slides. Read it aloud to rehearse. Every
sentence is meant to be spoken as written; adjust wording to your own voice
after the first rehearsal.

Conventions used below:

- `[0:00]` — the clock time the section should start at.
- **Show:** — what to put on the screen at that moment (Streamlit page,
  notebook, MLflow UI, a file). The talk assumes screen sharing throughout.
- Numbers in the text come from `docs/reports/model_evaluation_202601.md`
  and from the repository at the time of writing. If a number changes,
  update it here too.

Timing plan:

| section | minutes | starts at |
|---|---|---|
| Problem | 5 | 0:00 |
| Digital twin | 8 | 5:00 |
| Rebalancing: the decision the platform exists to test | 6 | 13:00 |
| Demand forecasting | 9 | 19:00 |
| The key idea: two-level evaluation | 9 | 28:00 |
| Operations loop and interfaces | 4 | 37:00 |
| Demo and limitations | 4 | 41:00 |

---

## [0:00] Problem (5 min)

**Show:** nothing yet, or the README front page.

I want to show you a project I built end to end: a platform for problems on
flow graphs — networks where some commodity moves between facilities. The
platform is general by design, but I want to be precise about its current
state up front: today exactly one domain is implemented, completely, from
raw data to a live app — the Citi Bike bike-sharing system in New York. The
plan is to grow by adding domains later, so everything I will show today is
told through that one domain.

Here is the operational problem. Citi Bike is a network of about 2,300
stations. In a busy month, people take almost five million trips. Every
trip moves a bike from one station to another, so the bikes drift: some
stations run empty, some fill up completely. When a station is empty, a
rider finds no bike — that is a lost trip, and I call it a stockout. When a
station is full, an arriving rider finds no free dock and has to ride
somewhere else — I call that a redirect. To fight this, the operator sends
trucks at night that move bikes from full stations back to empty ones.
Trucks, drivers, and fuel cost real money.

So the operator constantly faces decisions: how many trucks, which stations
to refill, to what level, is it worth reacting to a demand forecast at all.
And every one of those decisions raises the same question: **how do we test
a decision before spending money on it?** You cannot A/B test a truck
fleet on a real city.

My answer is a simulator that can replay the city's real history exactly —
and once it can do that, you can change one input, run it again, and read
the operational cost of the change from the output. The whole project is
organized around two runs: the base replay on real history, and a forecast
run where the only change is that demand comes from a machine-learning
model. The most interesting result of the project, and I will come back to
it in the middle of the talk, is this: **the model that wins on the usual
forecast metric is not the model that is cheapest to operate. You only see
that if you push every forecast through the simulator and compare costs.**

For scale: the repository holds fourteen consecutive months of real trip
data, February 2025 through March 2026, at hourly resolution, roughly 2,300
stations, and around 4.8 million trips in the busiest month.

## [5:00] Digital twin (8 min)

**Show:** `notebooks/test_pipeline.ipynb` — scroll to the run cell and the
totals; later in this section, the "Trips map" page in Streamlit.

By "digital twin" I mean something concrete: a simulator that, given the
real month's demand, reproduces exactly what happened — the same
departures, the same arrivals, zero losses. That exact replay is the
honesty check. If the simulator cannot reproduce the past, its answers
about the future are worthless. So the base replay is a first-class
artifact in the repo, and it must stay exact.

The design decision everything else rests on: **the simulator does not keep
an editable state table. It keeps a journal.** The flow journal is an
append-only table where each row is one event in the life of a flow: a bike
`departed`, `arrived`, was `redirected`, or was `lost`. Every other number
the project shows — station inventory over time, demand served, the
OD matrix, money — is computed from the journal by pure functions. Nothing
is stored twice.

Why this matters: in the rejected design, where you update inventory in
place, a missing or double-counted event silently changes the result and
there is nothing to check against. With the journal, every question about a
run — "did this demand depart, or become a stockout?" — has an answer in
rows. And it makes the run checkable. After every run the simulator
recomputes five invariants from the journal:

- demand equals departures plus stockout losses, per station and hour;
- every flow that is due by the end of the run closes with exactly one
  terminal event — arrived, redirected onward, or lost;
- the live inventory the engine kept for speed equals the inventory
  recomputed from the journal;
- bikes are conserved: what you started with equals what is docked at the
  end, plus what was lost to full docks, plus what is still riding;
- no station ever goes below zero bikes.

Every run I will show you today passed all five.

One more design decision worth a minute: **the initial state is measured,
not loaded.** A run needs to know how many bikes start at each station and
how many docks each station has. I do not load today's live station data —
today's inventory has nothing to do with a historical month, and using it
would create stockouts that never happened. Instead there is a sizing run:
the scenario runs once with effectively unlimited bikes and docks, and from
that run's journal I measure the smallest starting inventory under which no
station ever goes negative, and each station's peak occupancy becomes its
capacity. The state is derived from the demand it will face.

That measured state is also the lever that makes experiments possible. The
sizing demand and the run demand are two separate inputs. Equal inputs give
a clean replay. Size the state for history but feed the run a forecast —
and every stockout and redirect you see is caused by the forecast's error
and nothing else. Hold that thought; it is the mechanism behind the main
result.

One piece of mechanics before I move on, because it carries operational
cost: redirects. When a bike arrives at a full station, it goes to the
nearest station with a free dock; that bounce is recorded as a `redirected`
event plus a new leg of the trip. If no station anywhere has a free dock,
the bike is lost with reason `dock_full`. The other big piece of mechanics
is the night trucks — and that one deserves its own section.

**Show:** the "Trips map" page — one evening period, arcs colored by
outcome.

## [13:00] Rebalancing: the decision the platform exists to test (6 min)

**Show:** the "Truck trips" page — one night's truck moves on the map.

Now rebalancing, and I want to give it real time, because it is both the
most technically dense part of the simulator and the point of the whole
exercise. Remember the opening question: how do we test a decision before
spending money? The night trucks *are* that decision. Everything else in
the replay is fixed history — riders did what they did. The trucks are the
one thing the operator actually controls: how many, from where, moving
how many bikes, to which stations. So the simulator treats rebalancing not
as a side feature but as the experiment the platform exists to run: the
same month with and without trucks, or with two different truck policies,
and the journal prices the difference.

First, the design point. Trucks live in the same journal as riders. Every
bike a truck picks up becomes a flow with `flow_type="rebalance"`: a
`departed` event at pickup, an `arrived` event at dropoff, with the truck
recorded as the resource that carried it. There is no separate truck
subsystem with its own bookkeeping. That means the conservation invariant
covers truck bikes automatically — a bike on a truck is in transit exactly
like a bike on the road — and the cost of a night of rebalancing is
computed by the same pricing function as everything else: time in motion
times a rate. "Was last night worth it?" is a query over one table: truck
cost on one side, trips saved from stockout on the other.

Now the plan itself, once per night, four steps. Step one, the target: for
each station I ask what the next morning will do to it. I take the morning
window — six to noon — and compute the running net drain: cumulative
departures minus cumulative arrivals, hour by hour. The peak of that
running sum is the number of bikes the station must start the morning
with. Note it is a peak, not an average — a station that breaks even over
the morning but runs dry at eight thirty still loses the eight-thirty
riders. Step two, imbalance: current inventory minus target. Positive
stations are donors, negative stations need bikes. Two corrections keep
the plan physical: a station's planned inflow is clipped to its free
docks, and the two sides are trimmed to the same total, so the plan never
invents bikes. Step three, the imbalance is cut into portions of five
bikes — those portions are the stops the solver will route.

Step four is the vehicle-routing problem, solved with OR-Tools. The
objective is total truck travel time. The constraints: each truck has a
bike capacity and must end the window empty; every route must fit into a
two-hour night window; a stop costs two minutes plus half a minute per
bike handled. A stop may be skipped, but at a penalty so large the solver
only drops work that physically cannot fit in the window — I would rather
see an honest "we could not serve these stations tonight" than a plan that
pretends. Ten seconds of solver time per night is enough for a city of
this size.

And one honesty detail I like: planning and execution are two separate
phases, and execution does not trust the plan. The plan is built from the
state at one in the morning; by the time a truck reaches a station, the
world may have moved. So pickups take only the bikes actually standing
there, and if a dropoff arrives at a station that has meanwhile filled up,
the bikes that do not fit go back to the truck's home depot — recorded in
the journal like everything else. A plan on stale information degrades
visibly instead of corrupting the run.

Keep this section in mind for what comes next: the morning target is
computed from the demand table. In a replay that table is history — but in
a forecast run it is the model's output, which means **the demand model
literally decides where the trucks go**. That is the bridge from machine
learning to operations, and it is why forecast quality has to be measured
in truck miles and lost trips, not only in error metrics.

## [19:00] Demand forecasting (9 min)

**Show:** `gbp/ml/features.py` briefly; then the MLflow UI, backtest
experiment.

To ask about the future, the simulator needs a demand table for future
hours. Here the design rule is: **a forecast run replaces only the demand
table.** A model's output is a plain table — period, station, commodity,
quantity — with exactly the same schema as historical demand. It goes into
the one demand slot the engine reads. The simulator cannot tell a forecast
run from a replay, so the two runs differ in exactly one input, and any
difference in results is the forecast's doing.

Before any modeling, there is a data problem I want to highlight, because I
think it is the most research-flavored part: **censored demand.** The
training target is observed departures per station-hour. But when a station
stands empty, demand still exists — people walk up and leave — and the data
records zero. Observed departures are a lower bound on true demand exactly
at the station-hours where the system failed, which are the hours you most
want to predict. I handle it with an external signal: Citi Bike publishes a
live station-status feed, and archived dumps of it exist by month. I match
each station in my data to the nearest feed station within 100 meters and
compute, per station-hour, the fraction of the hour the station had zero
bikes available — a column called `stockout_share`. Models then weight each
training row by one minus `stockout_share`: an hour where the station was
empty half the time counts half as much. It is a correction of trust in the
label, not a deletion of data.

Features are deliberately simple, three groups: calendar — hour of day, day
of week, month, public holiday; daily weather — max and min temperature,
precipitation; and history — the same hour one week ago, the mean of the
last four same-hours-of-week, the station's overall mean, and the station's
hour-of-week mean, all computed from the eight weeks before the target.
There is a hard guard in the code: the history window must end strictly
before the target rows. No leakage by construction.

I compare four model families, ordered from simplest to most complex, all
behind one interface — fit, predict, save, load — so the backtest and the
simulator treat them identically:

- **Seasonal naive**: predict the station's hour-of-week mean. The
  baseline everything must beat.
- **SARIMAX**: a classical time-series model on the daily city-wide total,
  with weekly seasonality; the daily total is then split across stations
  and hours by their historical shares. It tests the idea "the city has one
  rhythm".
- **LightGBM**: one global gradient-boosting model over all stations, with
  a Poisson objective, station id as a categorical feature. The expected
  workhorse.
- **GraphSage**: a graph neural network where stations are nodes and edge
  weights are historical trip counts between them, trained with a Poisson
  loss. It tests whether spatial structure — what neighbors do — adds
  accuracy beyond boosting.

Selection is a rolling-origin backtest, which means: take the last three
months, hold each one out in turn, train on everything before it, predict
it. The training window expands, the test month always lies strictly after
it. Metrics are MAE and Poisson deviance, reported overall and separately
for busy stations — the smallest set of stations that produces half of all
departures — because an error at a busy station hurts operations more.
Every fold of every family is logged to MLflow: parameters, metrics, and
the model artifact itself.

**Show:** MLflow experiment table, the four families across splits.

## [28:00] The key idea: two-level evaluation (9 min)

**Show:** `docs/reports/model_evaluation_202601.md` — the two tables.

Now the part of the project I would defend as its main contribution. The
question "which model is best?" has two different answers depending on what
you measure, and I can show both on the same data.

The evaluation has two levels. Level one is the usual one: forecast error
against actual departures on a held-out window — January 2026, first week,
168 hourly periods. Level two is the unusual one: take each model's
forecast, push it through the simulator against a fixed state — the state
sized from the *actual* demand of that week, the same physical city for
every model — and compare the run totals against the reference run on
actual demand: how many trips departed, how many were lost, how many were
redirected, and what the week cost in dollars.

First, the trap I want you to see. In the backtest, on the full grid of
station-hours including all the zeros, GraphSage had the *best* MAE of the
four — 0.478 against 0.545 for LightGBM. If I had stopped at the forecast
metric, the graph neural network would be the champion. But most
station-hours in this system are zero, so a model biased toward predicting
near-zero everywhere scores well on MAE while being operationally useless.

Level two exposes it. Run through the simulator, GraphSage's forecast sends
only a third of the reference trips — minus 64 percent departures — and of
the trips it does send, 35 percent are lost anyway, because its demand sits
at the wrong stations. Its simulated cost is 62 percent below the
reference — not because it is efficient, but because it barely moves the
system. SARIMAX gets the January monthly total almost right, but the shape
inside the month is wrong — it misses the ramp-up after the New Year
holiday and loses a third of the departures. LightGBM lands closest to the
reference on both axes: departures within 7 percent, cost within 10
percent.

So the one-sentence version I want to leave with you: **a forecast metric
answers "how close are the numbers?"; the simulator answers "what would it
cost to operate on this forecast?" — and these two rankings disagree on
real data.** The forecast-metric winner, GraphSage, is the model the
simulator rejects hardest. That is why in this project the simulator, not
the metric, has the final word on model choice.

The decision that follows: LightGBM is the champion. I also state its known
flaw openly: it overpredicts volume by 18 to 22 percent on this window,
which the simulator converts into 9 percent of its trips lost and 5 percent
redirected. The next step is written down in the report — calibrate the
predicted totals and re-run this same evaluation. The evaluation is one
command, so re-running it after every model change is cheap.

**Show:** the level-2 table in the report; if time allows, the "Overview &
compare" page with the reference run and the LightGBM run side by side.

## [37:00] Operations loop and interfaces (4 min)

**Show:** MLflow model registry (champion alias); then the "Model
monitoring" page in Streamlit.

A chosen model degrades, so the project includes the loop that keeps it
honest, all runnable locally. One command — `python -m gbp.ml.pipeline` —
does five steps in order: download any newly published months of trip data
and station-status dumps; rebuild the missing monthly training partitions;
train a candidate model on all of it; backtest the candidate and the
current champion on the same splits; and decide promotion. The rule is
deliberately simple and written in one function: the candidate becomes
champion only if its mean MAE over the shared backtest splits is at least
as good as the champion's. The registry is MLflow with a movable
`champion` alias; the loser is tagged as challenger; every decision is
appended to a log file. Data versions are tracked with DVC, so a model
version is tied to the exact data it saw.

Between retrains there is monitoring: each month's saved forecast is scored
against the month's actuals, a rolling three-month MAE is compared against
the naive baseline — if the champion drops below naive, the month is
flagged as degraded — and an Evidently drift report compares the month's
feature distributions against the champion's training months, using
Wasserstein distance on the weather and history features. Calendar features
are excluded on purpose: hour-of-day cannot drift.

One slide's worth on who uses all this — three roles, three entry points:

- the **analyst** works in the Streamlit app: ten pages over saved run
  artifacts — compare two runs, maps, costs, monitoring;
- the **data scientist** works in the CLI and MLflow: `python -m
  gbp.ml.pipeline`, `forecast`, `evaluation`, `monitoring`, plus the two
  canonical notebooks — the base replay and the forecast run;
- the **integrator** works against a FastAPI service over the run
  artifacts: list runs, download any run table as parquet, and queue a new
  run over HTTP with an API key.

The app never computes anything the pipeline can precompute — it is a
reader of run artifacts. That keeps the boundary clean: one side produces
runs, the other side only displays them.

## [41:00] Demo and limitations (4 min)

**Show:** the Streamlit app, in this order — "Overview & compare",
"Station map", "Costs", "Model monitoring".

Ninety seconds of demo. On the compare page I pick the reference run and
the LightGBM forecast run — the totals you saw in the report, live. The
station map shows one hour of the city; hovering a station shows its
inventory, departures, and losses for both runs at once. The costs page
breaks the money down per period and per station. And the monitoring page
is the operations view: metric history per model version, degraded months,
drift reports.

I will close with the limitations, because I would rather state them than
be asked:

- The headline evaluation window is one week, and an unusual one — it
  starts on New Year's Day and demand triples within the week. The same
  command runs the full month; the numbers I show are the honest hard case.
- The OD matrix — where trips go and how long they take — is mapped from
  the same January the runs simulate. All models share it, so the
  comparison between them is fair, but a deployment would map it from
  earlier months.
- Weather features use the published weather of the target month — a
  perfect weather forecast. Fixing this means plugging in a real forecast
  feed.
- The MLOps loop is local-first: MLflow on SQLite, DVC, cron-able CLI. The
  cloud version exists as a written plan, not as running infrastructure.
- The champion's volume bias — plus 18 to 22 percent — is known and
  uncorrected; total calibration is the next modeling step.
- And one domain. That is a deliberate choice of depth over breadth: the
  domain vocabulary is already stripped out of the core — the code says
  commodity, facility, flow, not bike and station — so the path to a second
  domain is designed, but I refuse to claim it until it is built.

To summarize in one sentence: this is a platform where a demand model is
judged by the money its forecast would cost to operate on, not only by its
error metric — built end to end on one real city, with an exact replay as
the ground truth and an append-only journal that makes every number
checkable. Thank you — happy to go deeper into any part.
