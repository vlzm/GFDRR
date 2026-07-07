"""Pandera schema of the flow journal: its shape checked as data, not by hand.

:data:`FLOW_EVENT_SCHEMA` describes a finalized flow journal (Notations.md §0):
the column list and dtypes come from :data:`gbp.model.flows.FLOW_EVENT_DTYPES`,
so the layout is written once; the value checks are the journal's field
domains (``quantity >= 1``, the legal ``event_type`` / ``flow_type`` /
``reason`` sets) and the two cross-column rules (``move_id == event_id // 2``,
``realized_target_id`` set exactly on ``arrived`` rows).

The schema is checked at boundaries, once per journal: ``validate_run`` checks
a finished run's journal and ``get_historical_flows_df`` checks the historical
journal at load time. It is never run inside the per-period phase loop.

This module is deliberately not exported from ``gbp.model.__init__``: pandera
is a heavy import, so the two call sites import this module directly and
``import gbp.model`` stays cheap.
"""

import pandas as pd
import pandera.pandas as pa

from gbp.model.flows import FLOW_EVENT_COLUMNS, FLOW_EVENT_DTYPES

#: The four flow outcomes an event row can record (Notations.md §1).
EVENT_TYPES = ["departed", "arrived", "redirected", "lost"]
#: The two flow kinds: a user ride or a bike carried by a truck (Notations.md §0).
FLOW_TYPES = ["user_trip", "rebalance"]
#: The two loss reasons (Notations.md §1); every other row carries NA.
LOSS_REASONS = ["stockout", "dock_full"]

#: Columns that may be NA on a valid row. ``flow_id`` and the trip fields are
#: NA on an aggregated stockout loss; ``realized_*`` are NA until the flow
#: docks; ``resource_id`` is set only on rebalance flows; ``reason`` only on
#: ``redirected`` and ``lost`` rows.
_NULLABLE_COLUMNS = {
    "flow_id",
    "planned_target_id",
    "realized_target_id",
    "start_period",
    "planned_end_period",
    "realized_end_period",
    "resource_id",
    "reason",
}

#: Field-domain checks per column. Pandera skips NA values in these checks by
#: default, so ``reason`` reads as "in the legal set or NA".
_COLUMN_CHECKS = {
    "quantity": pa.Check.ge(1),
    "event_type": pa.Check.isin(EVENT_TYPES),
    "flow_type": pa.Check.isin(FLOW_TYPES),
    "reason": pa.Check.isin(LOSS_REASONS),
}


def _move_id_matches_event_id(flows: pd.DataFrame) -> pd.Series:
    """Arc ``m`` spans events ``2m`` and ``2m + 1``, so ``move_id == event_id // 2``."""
    return (flows["move_id"] == flows["event_id"] // 2).fillna(False)


def _realized_target_only_on_arrived(flows: pd.DataFrame) -> pd.Series:
    """``realized_target_id`` is set exactly on ``arrived`` rows (only docking sets it)."""
    return flows["realized_target_id"].notna() == (flows["event_type"] == "arrived")


#: The flow journal's schema: one column entry per ``FLOW_EVENT_COLUMNS``
#: (``ordered=True`` and ``strict=True`` pin the exact column list and order),
#: plus the two cross-column rules above.
FLOW_EVENT_SCHEMA = pa.DataFrameSchema(
    columns={
        column: pa.Column(
            FLOW_EVENT_DTYPES[column],
            checks=_COLUMN_CHECKS.get(column),
            nullable=column in _NULLABLE_COLUMNS,
        )
        for column in FLOW_EVENT_COLUMNS
    },
    checks=[
        pa.Check(
            _move_id_matches_event_id,
            name="move_id == event_id // 2",
            error="move_id disagrees with event_id (arc m spans events 2m and 2m+1)",
        ),
        pa.Check(
            _realized_target_only_on_arrived,
            name="realized_target_id set exactly on arrived",
            error="realized_target_id must be set on every arrived row and nowhere else",
        ),
    ],
    ordered=True,
    strict=True,
    name="flow_journal",
)


def schema_violations(schema: pa.DataFrameSchema, frame: pd.DataFrame) -> list[str]:
    """Run ``schema`` on ``frame`` with ``lazy=True``; return all violations as text.

    The shared bridge from pandera to the project's violation style: one check
    run reports every violation, as a list of human-readable strings, empty
    when the frame is valid (the same contract as the run invariants). Each
    failed check becomes one line with the failing column, the check, the row
    count, and up to three example values.
    """
    try:
        schema.validate(frame, lazy=True)
    except pa.errors.SchemaErrors as err:
        lines: list[str] = []
        for (column, check), group in err.failure_cases.groupby(
            ["column", "check"], dropna=False, sort=False
        ):
            where = column if pd.notna(column) else "table"
            examples = group["failure_case"].head(3).tolist()
            lines.append(
                f"{schema.name} schema: {where} failed '{check}' "
                f"on {len(group)} rows (e.g. {examples})"
            )
        return lines
    return []


def check_journal_schema(flows: pd.DataFrame) -> list[str]:
    """Return every way ``flows`` breaks the journal schema; empty when it holds.

    Parameters
    ----------
    flows : pandas.DataFrame
        A finalized flow journal (columns :data:`FLOW_EVENT_COLUMNS`).

    Returns
    -------
    list of str
        Human-readable violations; empty when the journal fits the schema.
    """
    return schema_violations(FLOW_EVENT_SCHEMA, flows)
