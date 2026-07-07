"""Structural well-formedness of a flow journal (a test-side invariant).

These checks are about the *shape* of the event log, independent of any
scenario. The row-wise part -- exact column list, dtypes, field domains,
``move_id`` agreeing with ``event_id`` -- is the pandera schema
(:func:`gbp.model.journal_schema.check_journal_schema`), shared with the
runtime. This module adds only what a row-wise schema cannot express: the
per-flow event sequence (ids contiguous, the legal event order, time never
running backwards inside a flow) and the stockout-loss shape. Both follow the
same contract: return a list of human-readable violations, empty when the
journal holds.
"""

import re

import pandas as pd

from gbp.model.journal_schema import check_journal_schema

#: A flow's event types, read in ``event_id`` order, must be: a ``departed``,
#: then zero or more bounce-and-new-leg pairs (``redirected``, ``departed``),
#: then at most one terminal (``arrived`` or ``lost``). No terminal means the
#: flow was still in transit when the run window ended.
LEGAL_FLOW_SHAPE = re.compile(r"departed(,redirected,departed)*(,(arrived|lost))?")


def check_journal_well_formed(flows: pd.DataFrame) -> list[str]:
    """Return every way ``flows`` breaks the journal's structural contract.

    Parameters
    ----------
    flows : pandas.DataFrame
        A finalized flow journal (e.g. ``Environment.simulated_flows_df``).

    Returns
    -------
    list of str
        Human-readable violations; empty when the journal is well-formed.
    """
    # The row-wise schema first; the per-flow checks below assume it holds
    # (they read the columns the schema guarantees).
    schema_violations = check_journal_schema(flows)
    if schema_violations:
        return schema_violations
    if flows.empty:
        return []

    v: list[str] = []

    # -- stockout losses are aggregated and id-less, at (move 0, event 0) ----
    stockout = flows[(flows["event_type"] == "lost") & (flows["reason"] == "stockout")]
    if stockout["flow_id"].notna().any():
        v.append("stockout loss carries a flow_id (it should be aggregated and id-less)")
    if not ((stockout["move_id"] == 0) & (stockout["event_id"] == 0)).all():
        v.append("stockout loss is not at (move 0, event 0)")

    # -- per-flow shape: the heart of the check ------------------------------
    flow_rows = flows[flows["flow_id"].notna()]
    # ``(flow_id, event_id)`` is the row key: it must be unique.
    dups = int(flow_rows.duplicated(["flow_id", "event_id"]).sum())
    if dups:
        v.append(f"id: {dups} duplicate (flow_id, event_id) pairs")
    for flow_id, group in flow_rows.groupby("flow_id"):
        group = group.sort_values("event_id")
        ids = group["event_id"].astype("int64").tolist()
        if ids != list(range(len(ids))):
            v.append(f"flow {flow_id}: event_id {ids} is not a contiguous 0..n")
        shape = tuple(group["event_type"].tolist())
        if not LEGAL_FLOW_SHAPE.fullmatch(",".join(shape)):
            v.append(f"flow {flow_id}: illegal event sequence {shape}")
        periods = group["period_id"].astype("int64").tolist()
        if any(earlier > later for earlier, later in zip(periods, periods[1:], strict=False)):
            v.append(f"flow {flow_id}: period_id runs backwards {periods}")
    return v
