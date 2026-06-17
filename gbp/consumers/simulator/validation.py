"""Run-level invariant checks (I1-I4) for a finished simulation.

Tier-2 of the loss-logging design: properties of the whole journal at run end,
too broad for a single phase's contract. ``validate_run`` runs once, behind
``EnvironmentConfig.validate`` -- off the hot path, always available, exercised
by the canonical notebook. I1/I2 are pure functions of the journal
(:mod:`gbp.model.journal`); I3/I4 also read the live final inventory, the
in-transit set and the initial inventory, so they live here in the simulator layer.

All checks return a list of human-readable violations (empty == holds);
``validate_run`` collects I1-I4 and the engine raises :class:`RunInvariantError`
if the combined list is non-empty. Like the rest of the constraint logic, every
invariant has no effect in an exact replay and only matters above the baseline.
"""

import pandas as pd

from gbp.loaders.dataloader_graph import ResolvedModelData
from gbp.model import check_demand_split, check_flow_closure, finalize_flows, get_inventory_df

from .state import SimulationState

_KEYS = ["facility_id", "commodity_category"]


class RunInvariantError(AssertionError):
    """Raised when a finished run violates one or more run invariants (I1-I4)."""


def validate_run(state: SimulationState, resolved: ResolvedModelData) -> list[str]:
    """Check invariants I1-I4 on a finished run; return all violations.

    Parameters
    ----------
    state : SimulationState
        The final simulation state (live inventory and in-transit set).
    resolved : ResolvedModelData
        The scenario inputs (initial inventory and historical demand).

    Returns
    -------
    list of str
        Human-readable invariant violations; empty when the run is valid.
    """
    flows = finalize_flows(state.state_flows_df)
    initial = resolved.initial_inventory_df

    violations: list[str] = []
    violations += check_demand_split(flows, resolved.historical_demand_df)  # I1
    violations += check_flow_closure(flows)  # I2
    violations += _check_projection_consistency(state.state_inventory_df, flows, initial)  # I3
    violations += _check_conservation(state, flows, initial)  # I4
    return violations


def _check_projection_consistency(
    live: pd.DataFrame, flows: pd.DataFrame, initial: pd.DataFrame
) -> list[str]:
    """I3 -- the live final inventory equals the inventory recomputed from the journal.

    Catches a docking event type the projection forgets to count (e.g. redirects):
    the live state would apply it, the journal projection would not, and the two
    inventories would diverge above the baseline.
    """
    if flows.empty:
        return []
    projected = get_inventory_df(flows, initial)
    last = int(projected["period_id"].max())
    proj = (projected[projected["period_id"] == last]
            .rename(columns={"quantity": "projected"})[_KEYS + ["projected"]])
    live = live.rename(columns={"quantity": "live"})[_KEYS + ["live"]]
    merged = (live.astype(dict.fromkeys(_KEYS, "string"))
              .merge(proj.astype(dict.fromkeys(_KEYS, "string")), on=_KEYS, how="outer")
              .fillna(0))
    bad = merged[merged["live"] != merged["projected"]]
    return [
        f"I3 {r.facility_id}/{r.commodity_category}: "
        f"live={int(r.live)} != journal={int(r.projected)}"
        for r in bad.itertuples(index=False)
    ]


def _check_conservation(
    state: SimulationState, flows: pd.DataFrame, initial: pd.DataFrame
) -> list[str]:
    """I4 -- bikes are conserved across inventory, dock-full losses and transit.

    ``Σ initial == Σ final_inventory + Σ lost(reason="dock_full") + Σ in_transit``.
    At run end a bike is either docked somewhere (inventory), gone from the system
    (lost to a full dock) or still riding because the run window ended mid-trip (in
    transit). A stockout bike never left a dock and a redirect keeps the bike in
    the system, so neither leaves the system.
    """
    initial_total = int(initial["quantity"].sum())
    final_total = int(state.state_inventory_df["quantity"].sum())
    lost_dock_full = flows[(flows["event_type"] == "lost") & (flows["reason"] == "dock_full")]
    lost_dock_full_total = int(lost_dock_full["quantity"].sum())
    transit_total = int(state.in_transit["quantity"].sum()) if not state.in_transit.empty else 0
    if initial_total != final_total + lost_dock_full_total + transit_total:
        return [
            f"I4 conservation: initial={initial_total} != final={final_total} + "
            f"lost_dock_full={lost_dock_full_total} + in_transit={transit_total}"
        ]
    return []
