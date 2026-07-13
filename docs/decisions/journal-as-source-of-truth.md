# The flow journal is the source of truth

## Decision

The simulator does not keep the run as one editable state table. It keeps
the flow journal — `state_flows_df`, an append-only table where each row is
one flow event (`departed`, `arrived`, `redirected`, `lost`). Everything
else the project shows — inventory, demand, maps, costs — is computed from
the journal by pure read-model functions in `gbp/model/flows.py`.

The live `state_inventory_df` and `in_transit` exist only for speed. They
are moved by the same events that go into the journal
(`SimulationState.apply_step_events`), and invariant I3 recomputes the
final inventory from the journal and compares the two.

## Rejected alternative

Update inventory in place and keep no event history. Then a missing or
double-counted event silently changes the run result, and there is nothing
to check the inventory against. With the journal, the event and the
inventory movement can always be checked against each other, and any
question about a run ("did this demand depart or become a stockout?") has
an answer in rows.

## Where in code

- `gbp/model/flows.py` — the event schema, builders, read-models.
- `gbp/consumers/simulator/state.py` — the one journal write path.
- `gbp/consumers/simulator/validation.py` — invariants I1–I5 recompute the
  run from the journal.
