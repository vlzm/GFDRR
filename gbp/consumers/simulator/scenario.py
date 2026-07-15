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

import pandas as pd
import structlog

from gbp.loaders.dataloader_graph import (
    FACILITIES_CAPACITIES_SCHEMA,
    INITIAL_INVENTORY_SCHEMA,
)
from gbp.model.journal_schema import schema_violations

from .config import EnvironmentConfig
from .engine import Environment
from .inputs import ScenarioInputs
from .phases import DockArrivals, FormDeparturesPhase, Phase
from .sizing import size_state_for_demand
from .state import SimulationState
from .validation import RunInvariantError

log = structlog.get_logger(__name__)


def canonical_phases() -> list[Phase]:
    """Build the canonical three-phase list every run of the scenario uses."""
    return [DockArrivals("previous"), FormDeparturesPhase(), DockArrivals("same")]


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
    sizing_config = EnvironmentConfig(
        phases=canonical_phases(),
        scenario_id=scenario_id,
        demand_scale_factor=sizing_scale_factor,
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
        sizing_data if sizing_data is not None else resolved, sizing_config
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

    sized = copy.copy(resolved)
    sized.initial_inventory_df = initial_inventory_df
    sized.facilities_capacities_df = facilities_capacities_df

    # The engine checks the invariants once and stores the result on
    # ``env.violations`` without raising, so the caller gets the violation list
    # either way and decides below whether a violation stops the run.
    run_config = EnvironmentConfig(
        phases=list(phases) if phases is not None else canonical_phases(),
        scenario_id=scenario_id,
        validate=True,
        demand_scale_factor=demand_scale_factor,
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
