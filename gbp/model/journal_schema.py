"""Pandera schema of the flow journal: its shape checked as data, not by hand."""

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
    """Run ``schema`` on ``frame`` with ``lazy=True``; return all violations as text."""
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
    """Return every way ``flows`` breaks the journal schema; empty when it holds."""
    return schema_violations(FLOW_EVENT_SCHEMA, flows)
