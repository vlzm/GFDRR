# Reference output: canonical four phases

The approved diagram for
`[DockArrivals("previous"), FormDeparturesPhase(), FormPotentialTripsPhase(), DockArrivals("same")]`
(the `phases_canonical` list from `notebooks/test_pipeline.ipynb`). New
diagrams must match this format.

```mermaid
%%{init: {"sequence": {"wrap": true, "noteAlign": "left", "actorMargin": 420, "noteMargin": 12}}}%%
sequenceDiagram
    participant INV as AT STATIONS<br/>(inventory)
    participant TR as IN TRANSIT<br/>(in_transit)

    loop each period t
        rect rgb(235, 245, 255)
            Note over INV,TR: PHASE 1 — DockArrivals("previous")
            Note over INV,TR: 📥 Reads<br/>state: in_transit → due[planned_end == t, start before t], inventory<br/>resolved: facilities_capacities_df, facilities_geo_df
            Note over INV,TR: ⚙️ Mechanics<br/>free = free_docks(inventory, capacities)<br/>(docked, overflow) = dock_up_to_capacity(due, free)<br/>(placed, lost) = plan_overflow_redirect(inventory´, capacities, geo, overflow)<br/>inventory´ — already includes docked
            TR->>INV: docked — arrived, +1 at planned target
            TR->>INV: placed — redirected, +1 at nearest neighbor
            Note over INV,TR: ✍️ Writes<br/>state: inventory += docked, then += placed<br/>state: in_transit −= due (lost rows included)<br/>SimulationLog: += arrived_events(docked) + redirected_events(placed)
            Note over INV,TR: ✅ Invariant<br/>count(due) == docked + placed + lost
        end
        rect rgb(235, 255, 235)
            Note over INV,TR: PHASE 2 — FormDeparturesPhase
            Note over INV,TR: 📥 Reads<br/>state: inventory<br/>resolved: historical_demand_df → demand_t[period_id == t]
            Note over INV,TR: ⚙️ Mechanics<br/>realized_df = realize_departures(demand_t, inventory)<br/>realized = min(demand, stock), lost = demand − realized
            INV->>INV: −realized
            Note over INV,TR: ✍️ Writes<br/>state: inventory −= realized<br/>state: intermediates["realized_departures"] = realized_df<br/>SimulationLog: nothing — no trips yet, counts only
            Note over INV,TR: ✅ Invariant<br/>demand == realized + lost, realized ≤ stock
        end
        rect rgb(255, 245, 230)
            Note over INV,TR: PHASE 3 — FormPotentialTripsPhase
            Note over INV,TR: 📥 Reads<br/>state: intermediates["realized_departures"]<br/>resolved: historical_od_matrix_df
            Note over INV,TR: ⚙️ Mechanics<br/>potential = form_potential_trips(realized_df, od, t)<br/>split by P(target | source, commodity), largest-remainder rounding, planned_end = t + pair duration<br/>trips = expand_potential_trips(potential, t)<br/>one row per bike, flow_id sim_*
            INV->>TR: departed — trips leave into transit
            Note over INV,TR: ✍️ Writes<br/>state: in_transit += trips<br/>SimulationLog: += departed_events(trips)<br/>state: inventory untouched — decremented in phase 2
            Note over INV,TR: ✅ Invariant<br/>sum(quantity over targets) == realized for each (source, commodity)
        end
        rect rgb(235, 245, 255)
            Note over INV,TR: PHASE 4 — DockArrivals("same")
            Note over INV,TR: same mechanics and reads/writes as phase 1, filter:<br/>state: in_transit → due[planned_end == t, start == t]
            TR->>INV: arrived / redirected
            Note over INV,TR: ✅ Invariant<br/>count(due) == docked + placed + lost
        end
    end
    Note over INV,TR: ✅ Run invariant (saturate_stock=True): lost == 0 everywhere → simulated_departures_df == historical_departures_df
```

## Verification nuances that accompanied this diagram

The post-diagram findings the user expects (real ones, checked against code):

- `plan_overflow_redirect` receives the inventory *already updated* with the
  docked flows of the same phase, and keeps its own running copy inside.
- `in_transit` drops ALL `due` rows, including `lost` — lost bikes vanish
  without a journal event.
- `form_potential_trips` drops rows with `probability.isna()`: a source absent
  from the OD matrix silently loses its realized departures, so the phase-3
  invariant has a hidden precondition (every demand source must be present in
  the OD matrix; true in the base replay by construction).
