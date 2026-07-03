"""Structural well-formedness of a flow journal (a test-side invariant).

These checks are about the *shape* of the event log, independent of any
scenario: the schema is exact, ids are unique, each flow's events follow the
legal sequence, ``move_id`` agrees with ``event_id``, and time never
runs backwards inside a flow. They complement the run-level invariants I1-I4
(:func:`gbp.consumers.simulator.validation.validate_run`), which are about
*quantities* (the demand split, conservation). Both follow the same contract:
return a list of human-readable violations, empty when the journal holds.

Kept in the test tree on purpose -- it is a tool for *checking* a journal in
tests, not part of the runtime contract. (If the schema contract ever needs
enforcing in production, promote it next to the other journal invariants.)
"""

import re

import pandas as pd

from gbp.model.flows import FLOW_EVENT_COLUMNS

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
    if list(flows.columns) != FLOW_EVENT_COLUMNS:
        return [f"schema: columns {list(flows.columns)} != FLOW_EVENT_COLUMNS"]
    if flows.empty:
        return []

    v: list[str] = []

    # -- field domains -------------------------------------------------------
    if (flows["quantity"].astype("int64") < 1).any():
        v.append("quantity: some rows are < 1")
    legal_types = {"departed", "arrived", "redirected", "lost"}
    unknown = set(flows["event_type"].dropna().unique()) - legal_types
    if unknown:
        v.append(f"event_type: unknown values {unknown}")
    # move_id is fixed by event_id: arc m opens at event 2m and ends at event 2m+1.
    expected_move = flows["event_id"].astype("int64") // 2
    if not (flows["move_id"].astype("int64") == expected_move).all():
        v.append("move_id: disagrees with event_id (arc m spans events 2m and 2m+1)")

    # -- realized_target_id is set only for a docking ``arrived`` ------------
    is_arrived = flows["event_type"] == "arrived"
    if flows.loc[is_arrived, "realized_target_id"].isna().any():
        v.append("realized_target_id: an ``arrived`` is missing its docked-at facility")
    if flows.loc[~is_arrived, "realized_target_id"].notna().any():
        v.append("realized_target_id: set on a non-``arrived`` event (only docking sets it)")

    # -- stockout losses are aggregated and id-less, at (move 0, event 0) ----
    stockout = flows[(flows["event_type"] == "lost") & (flows["reason"] == "stockout")]
    if stockout["flow_id"].notna().any():
        v.append("stockout loss carries a flow_id (it should be aggregated and id-less)")
    if not ((stockout["move_id"] == 0) & (stockout["event_id"] == 0)).all():
        v.append("stockout loss is not placed at (move 0, event 0)")

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
