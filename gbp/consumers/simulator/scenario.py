"""Size the state and run the canonical scenario."""

import copy
import dataclasses
import time
from collections.abc import Callable

import pandas as pd
import structlog

from gbp.model import CANONICAL_PHASE_ORDER
from gbp.model.dataloader_graph import (
    FACILITIES_CAPACITIES_SCHEMA,
    INITIAL_INVENTORY_SCHEMA,
)
from gbp.model.journal_schema import schema_violations

from .config import EnvironmentConfig
from .engine import Environment
from .inputs import ScenarioInputs
from .mechanics import scale_demand
from .phases import DockArrivals, FormDeparturesPhase, Phase
from .sizing import size_state_for_demand
from .state import SimulationState
from .validation import RunInvariantError

log = structlog.get_logger(__name__)


# How the simulator builds each phase named in gbp.model.CANONICAL_PHASE_ORDER.
# The map only picks the Phase for a name; the run order comes from
# CANONICAL_PHASE_ORDER itself, so the phase list cannot disagree with the ranks.
_PHASE_BUILDERS: dict[str, Callable[[], Phase]] = {
    "dock_previous": lambda: DockArrivals("previous"),
    "period_own": FormDeparturesPhase,
    "dock_same": lambda: DockArrivals("same"),
}


def canonical_phases() -> list[Phase]:
    """Build the canonical three-phase list that every scenario run uses."""
    return [_PHASE_BUILDERS[spec.name]() for spec in CANONICAL_PHASE_ORDER]


def scaled_demand_inputs(resolved: ScenarioInputs, factor: float) -> ScenarioInputs:
    """Return a copy of the inputs with the demand tables scaled (factor 1.0: unchanged)."""
    if factor <= 0:
        raise ValueError(f"demand scale factor must be > 0, got {factor}")
    if factor == 1.0:
        return resolved
    scaled = copy.copy(resolved)
    demand = resolved.historical_demand_df.copy()
    demand["quantity"] = scale_demand(demand["quantity"], factor)
    scaled.historical_demand_df = demand
    # Only the rebalancer's target inventory reads the arrivals table, so a
    # base run may not have it. When it is present, scale it like the demand.
    arrivals = getattr(resolved, "historical_arrivals_df", None)
    if arrivals is not None:
        arrivals = arrivals.copy()
        arrivals["quantity"] = scale_demand(arrivals["quantity"], factor)
        scaled.historical_arrivals_df = arrivals
    return scaled


@dataclasses.dataclass(frozen=True)
class ScenarioRun:
    """The result of one sized scenario run."""

    simulated_flows_df: pd.DataFrame
    state: SimulationState
    initial_inventory_df: pd.DataFrame
    facilities_capacities_df: pd.DataFrame
    violations: list[str]


def run_sized_scenario(
    resolved: ScenarioInputs,
    *,
    scenario_id: str,
    demand_scale_factor: float = 1.0,
    sizing_scale_factor: float = 1.0,
    number_of_periods: int,
    validate: bool = True,
    phases: list[Phase] | None = None,
    sizing_data: ScenarioInputs | None = None,
) -> ScenarioRun:
    """Size the state, run the scenario on it, and check the run invariants."""
    # Apply each run's demand scale to its data once, here. The sizing run and
    # the real run can have different scale factors, so each gets its own scaled
    # copy. After this point the engine and the phases see only scaled demand,
    # never the factor.

    sizing_inputs = scaled_demand_inputs(
        sizing_data if sizing_data is not None else resolved, sizing_scale_factor
    )

    run_inputs = scaled_demand_inputs(resolved, demand_scale_factor)

    sizing_config = EnvironmentConfig(
        phases=canonical_phases(),
        scenario_id=scenario_id,
        number_of_periods=number_of_periods,
    )
    log.info(
        "sizing_run_started",
        scenario_id=scenario_id,
        sizing_scale_factor=sizing_scale_factor,
        number_of_periods=number_of_periods,
    )

    sizing_started = time.monotonic()
    initial_inventory_df, facilities_capacities_df = size_state_for_demand(
        sizing_inputs, sizing_config
    )
    log.info(
        "sizing_run_finished",
        total_initial_inventory=int(initial_inventory_df["quantity"].sum()),
        facilities=len(facilities_capacities_df),
        elapsed_s=round(time.monotonic() - sizing_started, 1),
    )
    # The sized tables must match the schemas that the loader validated
    # for the original input tables.
    sizing_violations = [
        *schema_violations(INITIAL_INVENTORY_SCHEMA, initial_inventory_df),
        *schema_violations(FACILITIES_CAPACITIES_SCHEMA, facilities_capacities_df),
    ]
    if sizing_violations:
        raise ValueError("sized state tables break their schemas:\n" + "\n".join(sizing_violations))

    sized = copy.copy(run_inputs)
    sized.initial_inventory_df = initial_inventory_df
    sized.facilities_capacities_df = facilities_capacities_df

    # The engine checks the invariants and stores the result on
    # ``env.violations`` without raising. The caller always gets the violation
    # list; the ``validate`` flag below decides whether violations stop the run.
    run_config = EnvironmentConfig(
        phases=list(phases) if phases is not None else canonical_phases(),
        scenario_id=scenario_id,
        validate=True,
        number_of_periods=number_of_periods,
    )
    log.info(
        "run_started",
        scenario_id=scenario_id,
        demand_scale_factor=demand_scale_factor,
        number_of_periods=number_of_periods,
        phases=[type(p).__name__ for p in run_config.phases],
    )
    run_started = time.monotonic()
    env = Environment(sized, run_config)
    state = env.run()
    violations = env.violations
    simulated_flows_df = env.simulated_flows_df
    log.info(
        "run_finished",
        journal_rows=len(simulated_flows_df),
        violations=len(violations),
        elapsed_s=round(time.monotonic() - run_started, 1),
    )
    if validate and violations:
        raise RunInvariantError("run invariants violated:\n" + "\n".join(violations))

    return ScenarioRun(
        simulated_flows_df=simulated_flows_df,
        state=state,
        initial_inventory_df=initial_inventory_df,
        facilities_capacities_df=facilities_capacities_df,
        violations=violations,
    )
