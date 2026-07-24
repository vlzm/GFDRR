---
title: "Add a table to the run artifact"
weight: 4
---

# How to add a table to the run artifact

Task: make every saved run include a new precomputed table, so the web
interface and the API read it from disk instead of computing it.

The run artifact (`data/runs/<run_name>/`,
[Notations.md §12](../reference/notations.md#12-run-artifacts-the-files-the-ui-reads))
is the only thing the UI and the API read. Pages never compute what the
artifact builder can precompute — so a new derived table belongs in
`gbp/artifacts.py`, built once at save time from the journal.

## Steps

All edits are in `gbp/artifacts.py`.

1. Add the file stem to `RUN_TABLES`:

   ```python
   RUN_TABLES = ("flows", "panel", "arcs", "flow_totals", "facilities", "station_hours")
   ```

   Every reader keys on this tuple: `table_path` accepts only these stems,
   and the API endpoint `GET /runs/{run_name}/tables/{table}` (`app/api.py`)
   answers 404 for anything else.

2. Write a builder next to `build_arcs` and `build_flow_totals` — a function
   from the journal (and the other inputs `build_run_tables` receives) to one
   DataFrame — and add its result to the dict `build_run_tables` returns:

   ```python
   return {
       "flows": priced,
       ...
       "station_hours": build_station_hours(journal),
   }
   ```

3. Declare the table's pandera schema and register it in
   `RUN_TABLE_SCHEMAS`. `save_run` checks every table against its schema
   before writing anything, so a wrong column set fails at save time — not
   later, when a page tries to draw it.

4. Extend the artifact tests in `tests/test_app_artifacts.py` to cover the
   new table.

5. Re-run a scenario to produce an artifact with the new file:

   ```bash
   python app/runner.py --run-name with_new_table --periods 50
   ```

## Expected result

`data/runs/with_new_table/` contains `station_hours.parquet`, and both
readers return it with no further wiring:
`artifacts.load_run_table("with_new_table", "station_hours")` locally, and
`GET /runs/with_new_table/tables/station_hours` over the API.

Note: runs saved before the change do not have the new file, so
`load_run_table` fails on them with a missing-file error. Re-run the old
scenarios you still need, or make the page that reads the new table handle
its absence.
