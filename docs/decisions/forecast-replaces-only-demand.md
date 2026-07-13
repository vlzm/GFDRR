# A forecast run replaces only the demand table

## Decision

A forecast run ([Notations.md §11](../../Notations.md#11-run-kinds)) is the
same run chain as the base replay with one substitution:
`apply_forecast_demand` (`gbp/loaders/dataloader_graph.py`) returns a
shallow copy of the resolved data with the forecast demand table in the
`historical_demand_df` slot — the one demand slot the engine reads — plus
the matching period grid (`periods_df`, `t0`) and an OD matrix pooled from
history by hour of week. Facilities, capacities, routes, and the historical
observations stay shared with the original object.

The model's output is therefore a plain table —
`period_id, facility_id, commodity_category, quantity` — with the same
schema as historical demand. The simulator cannot tell a forecast run from
a replay.

## Rejected alternative

A separate forecast scenario builder or a second demand slot in the engine.
Either would split the run chain in two: two code paths to keep equal, and
run results that differ for reasons other than the demand itself. With one
slot, a forecast run and the reference run differ in exactly one input, so
any difference in the results is the forecast's doing.

## Where in code

- `gbp/loaders/dataloader_graph.py` — `apply_forecast_demand`,
  `map_od_matrix_by_hour_of_week`, `get_forecast_periods_df`.
- `app/runner.py` — `--demand-source forecast` loads the named forecast
  artifact and applies it before the run.
- `app/evaluate.py` — the two-level evaluation runs each model's forecast
  against the reference with the same replay state.
