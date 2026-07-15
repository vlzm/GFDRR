"""Run-level invariant checks (I1-I5) for a finished simulation.

Tier-2 of the loss-logging design: properties of the whole journal at run end,
too broad for a single phase's contract. ``validate_run`` runs once at the end
of ``Environment.run`` (on by default via ``EnvironmentConfig.validate``), so
the canonical run is checked every time. I1/I2 are pure functions of the journal
(:mod:`gbp.model.flows`); I3/I4/I5 also read the live final inventory, the
in-transit set and the initial inventory, so they live here in the simulator layer.
I5 is the step-contract guard: no inventory step takes a station below zero.

All checks return a list of human-readable violations (empty == holds);
``validate_run`` collects I1-I5 into that one list. The engine computes them at
end of run and stores them on ``Environment.violations``; the run entry point
(``run_sized_scenario``) raises :class:`RunInvariantError` when the list is
non-empty. Like the rest of the constraint logic, every invariant has no effect
in an exact replay and only matters above the baseline.
"""

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
from .mechanics import scale_demand
from .state import SimulationState

if TYPE_CHECKING:
    from .config import EnvironmentConfig

log = structlog.get_logger(__name__)

_KEYS = ["facility_id", "commodity_category"]


class RunInvariantError(AssertionError):
    """Raised when a finished run violates one or more run invariants (I1-I5)."""


def faced_demand(resolved: ScenarioInputs, config: EnvironmentConfig | None) -> pd.DataFrame:
    """Return the demand the run actually faced: scaled and cut to its horizon.

    The demand-split invariant I1 compares the journal against the demand the
    run saw, not the raw historical table. ``FormDeparturesPhase`` scales each
    period's demand by ``config.demand_scale_factor`` (the shared
    :func:`~gbp.consumers.simulator.mechanics.scale_demand` rule) and never sees
    periods at or past ``config.number_of_periods``. This applies the same two
    steps once, so the validator reads a value instead of redoing the engine's
    work with scalars a caller must keep in sync.

    Parameters
    ----------
    resolved : ScenarioInputs
        The scenario inputs; its ``historical_demand_df`` is the raw table.
    config : EnvironmentConfig or None
        The run's config. None means a plain full-grid replay (scale 1.0, the
        whole table) -- the default for a direct :func:`validate_run` call.

    Returns
    -------
    pandas.DataFrame
        The demand rows the run faced, with ``quantity`` already scaled.
    """
    demand = resolved.historical_demand_df
    if config is None:
        return demand
    demand = demand[demand["period_id"] < config.number_of_periods]
    if config.demand_scale_factor != 1.0:
        demand = demand.copy()
        demand["quantity"] = scale_demand(demand["quantity"], config.demand_scale_factor)
    return demand


def validate_run(
    state: SimulationState,
    resolved: ScenarioInputs,
    config: EnvironmentConfig | None = None,
) -> list[str]:
    """Check invariants I1-I5 on a finished run; return all violations.

    The five invariants: I1 — demand splits exactly into
    ``departed + lost(stockout)``; I2 — every departed flow due by run end
    closes with exactly one ``arrived`` or ``lost(dock_full)``; I3 — the live
    final inventory equals the inventory recomputed from the journal; I4 —
    bikes are conserved across final inventory, dock-full losses and
    ``in_transit``; I5 — no step takes a station's inventory below zero.

    Parameters
    ----------
    state : SimulationState
        The final simulation state (live inventory and in-transit set).
    resolved : ScenarioInputs
        The scenario inputs (initial inventory and historical demand).
    config : EnvironmentConfig or None, optional
        The config the run used. The demand-split check I1 compares the journal
        against the demand the run actually faced, which the config's
        ``demand_scale_factor`` and ``number_of_periods`` define
        (:func:`faced_demand`). Passing the config -- one object -- keeps the
        validator and the engine in step: a caller cannot hand over scalars
        that disagree with the run. None (the default) means a plain full-grid
        replay at scale 1.0.

    Returns
    -------
    list of str
        Human-readable invariant violations; empty when the run is valid.
        The list also covers the journal schema
        (:func:`gbp.model.journal_schema.check_journal_schema`), checked here
        once per run, before I1-I5.
    """
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
    """I3 -- the live final inventory equals the inventory recomputed from the journal.

    A safety check: since ``apply_step_events`` derives the live inventory from
    the events themselves (``inventory_deltas_from_events``), the two sides can
    only diverge if the incremental per-batch arithmetic and the full journal
    recomputation (``get_inventory_df``) disagree -- or if something writes the
    inventory outside the single write path.
    """
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


def _check_step_nonnegativity(flows: pd.DataFrame, initial: pd.DataFrame) -> list[str]:
    """I5 -- no inventory step drives a station's inventory below zero.

    This guards the step contract (Notations.md §0.1): one ``(period_id,
    phase_rank, phase_round)`` tuple is exactly one inventory batch. The journal
    cannot prove that contract directly -- the batch boundary is not stored -- but
    it catches the harmful case: if two batches that needed ordering collapse into
    one step (one phase emitting a second ordered batch under the same tuple),
    applying them as a single batch can take a station's inventory below zero. A
    bike cannot be docked or undocked at a station that has none, so any negative
    ``inventory_after`` is a real ordering bug, not just a bookkeeping one.
    """
    if flows.empty:
        return []
    moments = inventory_at_moments(flows, initial)
    bad = moments[moments["inventory_after"] < 0]
    return [
        f"I5 step {int(r.step_id)} {r.facility_id}/{r.commodity_category}: "
        f"inventory_after={int(r.inventory_after)} < 0"
        for r in bad.itertuples(index=False)
    ]
