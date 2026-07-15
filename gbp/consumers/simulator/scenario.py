"""The canonical scenario: its phase list and the size-then-run entry point.

Running a scenario correctly takes several steps in a fixed order: measure the
initial inventory and dock capacities with :func:`size_state_for_demand`, put
the measured state in place, run the real demand against it, and check the run
invariants. Doing these steps by hand in every caller invites mistakes:
building the :class:`Environment` before the sizing result is in place gives a
plausible-looking but wrong run. :func:`run_sized_scenario` is the one place
that owns this order; the terminal runner (``app/runner.py``) and the notebook
both call it.

The input ``resolved`` is never modified: the run happens on a shallow copy
that carries the sized tables, and the sized tables come back on the
:class:`ScenarioRun` result.
"""

import copy
import dataclasses
import time
from collections.abc import Callable

import pandas as pd
import structlog

from gbp.loaders.dataloader_graph import (
    FACILITIES_CAPACITIES_SCHEMA,
    INITIAL_INVENTORY_SCHEMA,
)
from gbp.model import CANONICAL_PHASE_ORDER
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
# The map says which Phase a name is; the order it runs in comes from the
# declaration, so the phase list and the historical ranks cannot disagree.
_PHASE_BUILDERS: dict[str, Callable[[], Phase]] = {
    "dock_previous": lambda: DockArrivals("previous"),
    "period_own": FormDeparturesPhase,
    "dock_same": lambda: DockArrivals("same"),
}


def canonical_phases() -> list[Phase]:
    """Build the canonical three-phase list every run of the scenario uses.

    The order is the one declared in :data:`gbp.model.CANONICAL_PHASE_ORDER`, so
    it is authored once. The engine also checks the built list is sorted by
    ``phase_rank``, which confirms the two agree.
    """
    return [_PHASE_BUILDERS[spec.name]() for spec in CANONICAL_PHASE_ORDER]


def scaled_demand_inputs(resolved: ScenarioInputs, factor: float) -> ScenarioInputs:
    """Bind the run's demand scale into the scenario tables, once, at the boundary.

    The run faces a scaled version of the demand. Instead of every phase, the
    validator and the rebalancer each rescaling the raw table with the same
    factor, the scale is applied here once and the whole run reads the demand it
    actually faces. Two tables carry the demand: ``historical_demand_df`` (the
    departures ``FormDeparturesPhase`` forms) and ``historical_arrivals_df`` (the
    arrivals the rebalancer's ``target_inventory`` reads); both scale by the same
    factor. Rounding is per row (:func:`~gbp.consumers.simulator.mechanics.scale_demand`),
    so scaling the whole table once matches scaling each period in turn.

    ``resolved`` is not modified: the scaled tables land on a shallow copy.
    ``factor == 1.0`` returns ``resolved`` unchanged (no copy).

    Parameters
    ----------
    resolved : ScenarioInputs
        The scenario inputs to scale. Every table other than the two demand
        tables is shared as-is.
    factor : float
        The demand multiplier; must be positive.

    Returns
    -------
    ScenarioInputs
        The inputs the run faces, with the two demand tables scaled.
    """
    if factor <= 0:
        raise ValueError(f"demand scale factor must be > 0, got {factor}")
    if factor == 1.0:
        return resolved
    scaled = copy.copy(resolved)
    demand = resolved.historical_demand_df.copy()
    demand["quantity"] = scale_demand(demand["quantity"], factor)
    scaled.historical_demand_df = demand
    # The arrivals table is read only by the rebalancer's target inventory, so a
    # base run may not carry it. Scale it when it is there, matching the demand.
    arrivals = getattr(resolved, "historical_arrivals_df", None)
    if arrivals is not None:
        arrivals = arrivals.copy()
        arrivals["quantity"] = scale_demand(arrivals["quantity"], factor)
        scaled.historical_arrivals_df = arrivals
    return scaled


@dataclasses.dataclass(frozen=True)
class ScenarioRun:
    """Everything a finished sized run hands back to its caller (Notations.md §11).

    The finalized journal, the final simulation state, the sized state tables
    the run started from, and the invariant violations (empty = valid).
    """

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
    """Size the state, run the scenario against it, check the run invariants.

    The state (initial inventory and dock capacities) is sized against
    ``sizing_scale_factor``; the run itself faces ``demand_scale_factor``.
    Equal values give a clean, no-loss run; a larger run scale makes the
    limits take effect (stockout and dock-full events appear).

    Parameters
    ----------
    resolved : ScenarioInputs
        The scenario inputs. Not modified: the run works on a shallow copy
        that carries the sized tables.
    scenario_id : str
        Scenario id stamped on the sizing and run configs.
    demand_scale_factor : float, optional
        Demand multiplier the run faces.
    sizing_scale_factor : float, optional
        Demand multiplier the state is sized to survive with no loss.
    number_of_periods : int
        How many periods to step.
    validate : bool, optional
        When True (default), raise :class:`RunInvariantError` if the finished
        run violates the run invariants I1-I5. When False, the violations are
        only recorded on the result (the runner stores them in ``meta.json``).
    phases : list of Phase, optional
        The phase list the *run* uses. Default: ``canonical_phases()``. Pass
        ``canonical_phases() + rebalancing_phases(params)`` to run with the
        overnight rebalancing (Notations.md §14). The sizing run always uses
        the canonical three phases -- the state is sized for the demand alone,
        so the rebalancer's effect shows up against it instead of being sized
        away.
    sizing_data : ScenarioInputs, optional
        The scenario data the sizing run measures. Default: ``resolved``
        itself, which gives a clean run. Passing different data sizes the
        state on one demand table while the run faces another; the gap
        between the two shows up as lost and redirected events. The
        two-level evaluation uses this for its replay-state forecast runs
        (Notations.md §11): the state is sized on the actual demand
        (``sizing_data``), the run faces a forecast (``resolved``). The two
        must describe the same scenario — same facilities, period grid, and
        OD matrix — or the sized state is meaningless.

    Returns
    -------
    ScenarioRun
        The finalized journal, the final state, the sized state tables, and
        the invariant violations (empty = valid).
    """
    # Bind each run's demand scale into its data once, here at the boundary. The
    # sizing run and the real run face different scales, so they get different
    # scaled copies; from here on the engine and the phases read the demand they
    # face, never the scale.
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

    # The sized tables replace two engine inputs, so they must fit the same
    # schemas the loader checked the originals against.
    sizing_violations = [
        *schema_violations(INITIAL_INVENTORY_SCHEMA, initial_inventory_df),
        *schema_violations(FACILITIES_CAPACITIES_SCHEMA, facilities_capacities_df),
    ]
    if sizing_violations:
        raise ValueError("sized state tables break their schemas:\n" + "\n".join(sizing_violations))

    sized = copy.copy(run_inputs)
    sized.initial_inventory_df = initial_inventory_df
    sized.facilities_capacities_df = facilities_capacities_df

    # The engine checks the invariants once and stores the result on
    # ``env.violations`` without raising, so the caller gets the violation list
    # either way and decides below whether a violation stops the run.
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
