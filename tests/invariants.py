"""Structural well-formedness of a flow journal (a test-side invariant).

These checks are about the *shape* of the event log, independent of any
scenario: the schema is exact, ids are unique, each flow's events follow one of
the four legal sequences, ``move_id`` agrees with ``event_id``, and time never
runs backwards inside a flow. They complement the run-level invariants I1-I4
(:func:`gbp.consumers.simulator.validation.validate_run`), which are about
*quantities* (the demand split, conservation). Both follow the same contract:
return a list of human-readable violations, empty when the journal holds.

Kept in the test tree on purpose -- it is a tool for *checking* a journal in
tests, not part of the runtime contract. (If the schema contract ever needs
enforcing in production, promote it next to the other journal invariants.)
"""

import pandas as pd

from gbp.model.flows import FLOW_EVENT_COLUMNS

#: The only event-type sequences a single flow may show, read in ``event_id``
#: order. A lone ``departed`` is a flow still in transit when the run window
#: ended; the four-event form is a redirect (bounce, then a second arc).
LEGAL_FLOW_SHAPES = {
    ("departed",),
    ("departed", "arrived"),                            # normal trip
    ("departed", "lost"),                               # dock-full loss
    ("departed", "redirected", "departed", "arrived"),  # redirect: two arcs
}


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
    if not flows["move_id"].dropna().astype("int64").isin([0, 1]).all():
        v.append("move_id: values outside {0, 1}")
    # move_id is fixed by event_id: events 0-1 are arc 0, events 2-3 are arc 1.
    expected_move = (flows["event_id"].astype("int64") > 1).astype("int64")
    if not (flows["move_id"].astype("int64") == expected_move).all():
        v.append("move_id: disagrees with event_id (events 0-1 -> arc 0, 2-3 -> arc 1)")

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
        if shape not in LEGAL_FLOW_SHAPES:
            v.append(f"flow {flow_id}: illegal event sequence {shape}")
        periods = group["period_id"].astype("int64").tolist()
        if any(earlier > later for earlier, later in zip(periods, periods[1:], strict=False)):
            v.append(f"flow {flow_id}: period_id runs backwards {periods}")
    return v
