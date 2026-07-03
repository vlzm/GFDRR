"""Sizing run: measure the initial inventory and capacities a scenario needs.

The replay-minimal initial inventory (``get_replay_initial_inventory_df``) is
sized against the *historical* journal, so it has no slack: raise the demand
even a little and stockouts appear. A scaled run is also not "history times k" —
the demand is rounded per ``(period, facility, commodity)``, the departures are
re-spread over targets by the OD matrix, and the arrival periods come from mean
durations. So the only journal the scaled scenario can be sized against is its
own.

That is what the sizing run does: run the scenario once with **saturated**
initial inventory and dock capacities (both far above any demand, so no
departure is lost and no arrival is redirected), then apply the same two sizing
functions the clean replay uses to the journal of that run. Because every phase
is deterministic and departures depend on inventory only through
``min(demand, inventory)``, a real run started from the measured state repeats
the sizing run's journal exactly — zero stockout, zero dock-full.
"""

import copy
import dataclasses

import pandas as pd

from gbp.loaders.dataloader_graph import (
    ResolvedModelData,
    get_replay_capacities_df,
    get_replay_initial_inventory_df,
    get_saturated_inventory_df,
)

from .config import EnvironmentConfig
from .engine import Environment

#: Per-station inventory and per-facility capacity used in the sizing run.
#: Far above any period's demand, so no limit ever takes effect.
SATURATION_QUANTITY = 1_000_000


def size_state_for_demand(
    resolved: ResolvedModelData,
    config: EnvironmentConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Smallest initial inventory and capacities for this config's demand.

    Runs the sizing run (see the module docstring): the scenario in ``config`` —
    including its ``demand_scale_factor`` and ``number_of_periods`` — against a
    saturated copy of ``resolved``, and reads the requirements from the
    resulting journal with :func:`get_replay_initial_inventory_df` and
    :func:`get_replay_capacities_df`.

    ``resolved`` is not modified; assign the returned tables to
    ``resolved.initial_inventory_df`` and ``resolved.facilities_capacities_df``
    before building the real :class:`Environment`.

    Parameters
    ----------
    resolved : ResolvedModelData
        The scenario data to size. Only ``initial_inventory_df`` and
        ``facilities_capacities_df`` are replaced for the sizing run; every
        other table is shared as-is.
    config : EnvironmentConfig
        The configuration the real run will use. The sizing run uses the same
        phases, demand scale, and period count, so the measured state matches
        the run it is meant for.

    Returns
    -------
    tuple of (pandas.DataFrame, pandas.DataFrame)
        ``initial_inventory_df`` (``facility_id``, ``commodity_category``,
        ``quantity``) and ``facilities_capacities_df`` (``facility_id``,
        ``capacity``).
    """
    saturated = copy.copy(resolved)
    saturated.initial_inventory_df = get_saturated_inventory_df(
        resolved.facilities_df, resolved.commodities_categories_df, SATURATION_QUANTITY
    )
    # The saturated inventory holds SATURATION_QUANTITY per commodity, so a
    # station's occupancy starts at n_commodities x SATURATION_QUANTITY. The
    # saturated capacity must sit above that plus every arrival the run could
    # dock, or the stations would read as full from the first moment and every
    # arrival would redirect — poisoning the journal being measured.
    n_commodities = len(resolved.commodities_categories_df)
    saturated.facilities_capacities_df = resolved.facilities_capacities_df[["facility_id"]].assign(
        capacity=(n_commodities + 1) * SATURATION_QUANTITY
    )

    # The sizing run's own guard is the no-lost/no-redirected assert below, so
    # the run-level invariant pass is skipped here to keep the sizing run cheap.
    sizing_config = dataclasses.replace(
        config, scenario_id=config.scenario_id + "_sizing", validate=False
    )
    env = Environment(saturated, sizing_config)
    env.run()
    flows = env.simulated_flows_df
    # The measurement is only valid if no limit took effect in the sizing run:
    # a lost or redirected event here means the saturation was not high enough.
    bad = flows["event_type"].isin(["lost", "redirected"])
    assert not bad.any(), f"sizing run is not saturated: {int(bad.sum())} lost/redirected events"

    initial_inventory_df = get_replay_initial_inventory_df(
        flows, resolved.facilities_df, resolved.commodities_categories_df
    )
    facilities_capacities_df = get_replay_capacities_df(
        flows, initial_inventory_df, resolved.facilities_capacities_df
    )
    return initial_inventory_df, facilities_capacities_df
