# Trip distance and duration

How the codebase computes trip length in time and in space.

**Duration** drives the simulation (periods). **Distance in kilometres** is computed for analysis on top of the flow journal, and in one simulation case: it estimates a redirect leg's travel time when the OD matrix has no entry for the pair. **Squared Euclidean distance on coordinates** is used only to rank neighbour stations during overflow redirect.

## Duration

The unit is the **simulation period**, not wall-clock seconds. The default period length is one hour (`DEFAULT_PERIOD_LEN` in `gbp/loaders/dataloader_graph.py`).

### Historical trips

Raw Citi Bike CSV rows supply `started_at` and `ended_at`. They are mapped to period ids with `to_period_id`:

```python
start_period = to_period_id(started_at, t0, period_len)
planned_end_period = to_period_id(ended_at, t0, period_len)
```

Trip duration in periods:

```
duration = planned_end_period - start_period
```

This is integer division of elapsed time into buckets of length `period_len`.

### Simulated trips (OD matrix)

The OD demand model is built from user departures in the historical journal (`flows_to_od_matrix` in `gbp/model/flows.py`):

1. For each departure: `duration = planned_end_period - start_period`
2. Group by `(source_id, planned_target_id, period_id, commodity_category)`
3. Take the mean `duration`, round to a whole period

When `form_potential_trips` creates new trips:

```python
planned_end_period = period_id + duration
```

where `period_id` is the departure period and `duration` comes from the OD matrix for that source–target pair.

### Redirect legs (overflow)

If a bike cannot dock at its target, `plan_overflow_redirect` plans a new leg. Travel time uses `_leg_durations`: the mean `duration` from the OD matrix for the pair `(planned_target_id, realized_target_id)`.

If that pair never appeared in history, the travel time is estimated from distance and speed:

```
duration = round(haversine_km(pair) / trip_speed_km_per_period)
```

`trip_speed_km_per_period` is the mean riding speed over all historical trips: total great-circle distance divided by total ride time (`get_trip_speed_km_per_period` in `gbp/loaders/dataloader_graph.py`). The speed comes from the raw `started_at` / `ended_at` timestamps, not from the OD matrix — OD durations are rounded to whole periods and most trips are shorter than one period, so a speed computed from them would divide by near-zero times. Trips with the same start and end station, zero ride time, or a missing coordinate are skipped.

With one-hour periods the estimate rounds to 0 for any leg shorter than half the hourly speed (roughly 7 km at typical riding speed), so nearby redirects still dock in the same period; only a leg to a far station takes extra periods.

### Post-hoc fields in `get_flows_wide`

After joining geo and facility attributes:

- `planned_duration = planned_end_period - start_period`
- `realized_duration = realized_end_period - start_period`

## Distance

Travel time comes from the OD matrix when the pair has an entry. Distance sets travel time only in the redirect fallback above.

### Kilometres (Haversine)

Great-circle distance in km between station coordinates: `haversine_km` in `gbp/model/flows.py`, Earth radius 6371.0088 km. Used in two places:

- Analysis: `get_flows_wide` (`gbp/loaders/dataloader_graph.py`) adds `planned_distance_km` (source → planned target) and `realized_distance_km` (source → realized target, after redirect).
- Simulation: the redirect travel-time fallback above, and the mean speed it divides by.

### Squared coordinate distance — redirect ranking only

`neighbor_distance_sq` in `gbp/model/flows.py`:

```python
(lat - other_lat) ** 2 + (lng - other_lng) ** 2
```

Used to pick the nearest free station and to explain redirects (`redirect_neighbor_table`). Not stored as trip distance in the journal.

## Summary

| Quantity | Formula / source | Used in simulation? |
|----------|------------------|---------------------|
| Trip duration (periods) | `planned_end_period - start_period` | Yes |
| Simulated travel time | Mean OD `duration` per station pair | Yes |
| Redirect travel time, pair not in OD | `round(haversine_km / trip_speed_km_per_period)` | Yes |
| Mean riding speed | Total haversine km / total ride time, from raw timestamps | Yes (redirect fallback) |
| Trip distance (km) | Haversine on lat/lng | Analysis + redirect fallback |
| Neighbour rank | Squared Δlat² + Δlng² | Redirect choice only |

## Key code locations

| Topic | Module / function |
|-------|-------------------|
| Period grid, historical periods | `gbp/loaders/dataloader_graph.py` — `to_period_id`, `get_historical_flows_df` |
| OD duration | `gbp/model/flows.py` — `flows_to_od_matrix` |
| Simulated departures | `gbp/consumers/simulator/mechanics.py` — `form_potential_trips` |
| Redirect travel time | `gbp/consumers/simulator/mechanics.py` — `_leg_durations`, `plan_overflow_redirect` |
| Mean riding speed | `gbp/loaders/dataloader_graph.py` — `get_trip_speed_km_per_period` |
| Great-circle distance | `gbp/model/flows.py` — `haversine_km` |
| Wide panel km + duration columns | `gbp/loaders/dataloader_graph.py` — `get_flows_wide` |
| Redirect neighbour metric | `gbp/model/flows.py` — `neighbor_distance_sq` |
