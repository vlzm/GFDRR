"""Run-level invariant checks (I1-I5) for a finished simulation."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import structlog

from gbp.model import (
    check_demand_split,
    check_flow_closure,
    finalize_flows,
    get_inventory_df,
    inventory_at_moments,
)
from gbp.model.journal_schema import check_journal_schema

from .inputs import ScenarioInputs
from .state import SimulationState

if TYPE_CHECKING:
    from .config import EnvironmentConfig

log = structlog.get_logger(__name__)

_KEYS = ["facility_id", "commodity_category"]


class RunInvariantError(AssertionError):
    """Raised when a finished run violates one or more run invariants (I1-I5)."""


def faced_demand(resolved: ScenarioInputs, config: EnvironmentConfig | None) -> pd.DataFrame:
    """Return the demand the run actually faced: the table cut to its horizon."""
    demand = resolved.historical_demand_df
    if config is None:
        return demand
    return demand[demand["period_id"] < config.number_of_periods]


def validate_run(
    state: SimulationState,
    resolved: ScenarioInputs,
    config: EnvironmentConfig | None = None,
) -> list[str]:
    """Check invariants I1-I5 (and the journal schema) on a finished run; return all violations."""
    flows = finalize_flows(state.state_flows_df)
    initial = resolved.initial_inventory_df
    demand = faced_demand(resolved, config)

    violations: list[str] = []
    violations += check_journal_schema(flows)  # the journal's shape contract
    violations += check_demand_split(flows, demand)  # I1
    violations += check_flow_closure(flows)  # I2
    violations += _check_projection_consistency(state.state_inventory_df, flows, initial)  # I3
    violations += _check_conservation(state, flows, initial)  # I4
    violations += _check_step_nonnegativity(flows, initial)  # I5
    if violations:
        log.warning("run_invariants_violated", count=len(violations), first=violations[:3])
    else:
        log.info("run_invariants_checked", violations=0)
    return violations


def _check_projection_consistency(
    live: pd.DataFrame, flows: pd.DataFrame, initial: pd.DataFrame
) -> list[str]:
    """I3 -- the live final inventory equals the inventory recomputed from the journal."""
    if flows.empty:
        return []
    projected = get_inventory_df(flows, initial)
    last = int(projected["period_id"].max())
    proj = projected[projected["period_id"] == last].rename(columns={"quantity_eop": "projected"})[
        _KEYS + ["projected"]
    ]
    live = live.rename(columns={"quantity": "live"})[_KEYS + ["live"]]
    merged = (
        live.astype(dict.fromkeys(_KEYS, "string"))
        .merge(proj.astype(dict.fromkeys(_KEYS, "string")), on=_KEYS, how="outer")
        .fillna(0)
    )
    bad = merged[merged["live"] != merged["projected"]]
    return [
        f"I3 {r.facility_id}/{r.commodity_category}: "
        f"live={int(r.live)} != journal={int(r.projected)}"
        for r in bad.itertuples(index=False)
    ]


def _check_conservation(
    state: SimulationState, flows: pd.DataFrame, initial: pd.DataFrame
) -> list[str]:
    """I4 -- bikes are conserved across inventory, dock-full losses and transit."""
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


def _check_step_nonnegativity(flows: pd.DataFrame, initial: pd.DataFrame) -> list[str]:
    """I5 -- no inventory step drives a station's inventory below zero."""
    if flows.empty:
        return []
    moments = inventory_at_moments(flows, initial)
    bad = moments[moments["inventory_after"] < 0]
    return [
        f"I5 step {int(r.step_id)} {r.facility_id}/{r.commodity_category}: "
        f"inventory_after={int(r.inventory_after)} < 0"
        for r in bad.itertuples(index=False)
    ]
