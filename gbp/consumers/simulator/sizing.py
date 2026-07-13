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

from gbp.loaders.dataloader_graph import get_replay_initial_inventory_df
from gbp.model import inventory_at_moments, occupancy_per_facility

from .config import EnvironmentConfig
from .engine import Environment
from .inputs import ScenarioInputs

#: Per-station inventory and per-facility capacity used in the sizing run.
#: Far above any period's demand, so no limit ever takes effect.
SATURATION_QUANTITY = 1_000_000


def get_saturated_inventory_df(
    facilities_df: pd.DataFrame,
    commodities_categories_df: pd.DataFrame,
    quantity: int = SATURATION_QUANTITY,
) -> pd.DataFrame:
    """Build artificial initial inventory holding ``quantity`` bikes per station.

    Every station holds ``quantity`` bikes of each commodity. The sizing run
    starts from this table instead of a snapshot of today's real station
    inventory. Such a snapshot is a *current* observation, unrelated to the
    historical start state, so limiting demand against it starves the run (most
    departures lose to a stockout that never happened historically). With
    inventory far above any period's demand the limit never takes effect and
    every departure departs, which is what makes the run's journal a valid
    measurement.

    Returns
    -------
    pandas.DataFrame
        ``facility_id``, ``commodity_category``, ``quantity`` for every
        ``(station, commodity)``.
    """
    stations = facilities_df.loc[facilities_df["facility_category"] == "station", ["facility_id"]]
    inv = stations.merge(commodities_categories_df[["commodity_category"]], how="cross")
    inv["quantity"] = quantity
    return inv.reset_index(drop=True)


def get_replay_capacities_df(
    historical_flows_df: pd.DataFrame,
    initial_inventory_df: pd.DataFrame,
    facilities_capacities_df: pd.DataFrame,
    min_capacity: int = 10,
) -> pd.DataFrame:
    """Smallest dock capacities that let the replay run with no dock-full/redirect.

    The mirror of
    :func:`gbp.loaders.dataloader_graph.get_replay_initial_inventory_df`. Dock
    capacity is per facility, shared across commodities, and a dock-full (then
    a redirect) happens when incoming bikes would push the facility's total
    occupancy above its capacity. With the initial inventory fixed,
    :func:`gbp.model.inventory_at_moments` gives the occupancy after every
    step; the per-facility peak of the total across commodities
    (:func:`gbp.model.occupancy_per_facility`) is the most docks ever needed at
    once. A capacity equal to that peak holds every arrival, so no flow is ever
    redirected.

    The peak must include the *initial* occupancy (the moment before the first
    step), not only the after-step values. A station whose inventory only drains
    early on has its all-time high at the start; taking the peak over after-step
    values alone would set its capacity below the bikes it already holds, so its
    free docks would read as zero and every arrival there would redirect.

    Parameters
    ----------
    historical_flows_df : pandas.DataFrame
        The flow log to size against (historical, or a sizing run's journal).
    initial_inventory_df : pandas.DataFrame
        The start inventory to size against (use the output of
        :func:`gbp.loaders.dataloader_graph.get_replay_initial_inventory_df`).
    facilities_capacities_df : pandas.DataFrame
        The capacity table whose ``facility_id`` set defines the output rows.
    min_capacity : int, optional
        A floor applied to every facility, so facilities with no replay traffic
        (e.g. depots) keep a usable capacity. Defaults to 10.

    Returns
    -------
    pandas.DataFrame
        ``facility_id`` and ``capacity`` set to each facility's required peak,
        floored at ``min_capacity``.
    """
    moments = inventory_at_moments(historical_flows_df, initial_inventory_df)
    step_totals = occupancy_per_facility(moments, "inventory_after", extra_keys=("step_id",))
    step_peak = step_totals.groupby("facility_id").max()
    # The initial occupancy is the moment before the first step; a facility whose
    # inventory only drains has its all-time high here, not at any after-step value.
    initial_total = occupancy_per_facility(initial_inventory_df)
    idx = step_peak.index.union(initial_total.index)
    peak = (
        pd.concat(
            [step_peak.reindex(idx, fill_value=0), initial_total.reindex(idx, fill_value=0)],
            axis=1,
        )
        .max(axis=1)
        .rename("peak_occupancy")
        .rename_axis("facility_id")
        .reset_index()
    )

    out = facilities_capacities_df[["facility_id"]].merge(peak, on="facility_id", how="left")
    out["capacity"] = out["peak_occupancy"].fillna(0).clip(lower=min_capacity).astype("int64")
    return out[["facility_id", "capacity"]]


def size_state_for_demand(
    resolved: ScenarioInputs,
    config: EnvironmentConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Smallest initial inventory and capacities for this config's demand.

    Runs the sizing run (see the module docstring): the scenario in ``config`` —
    including its ``demand_scale_factor`` and ``number_of_periods`` — against a
    saturated copy of ``resolved``, and reads the requirements from the
    resulting journal with :func:`get_replay_initial_inventory_df` and
    :func:`get_replay_capacities_df`. The saturated inventory is
    :func:`get_saturated_inventory_df`; the saturated capacity is
    ``(n_commodities + 1) * SATURATION_QUANTITY`` docks per facility, above
    the starting occupancy plus every arrival the run could dock, so nothing
    redirects during the measurement.

    ``resolved`` is not modified; assign the returned tables to
    ``resolved.initial_inventory_df`` and ``resolved.facilities_capacities_df``
    before building the real :class:`Environment`.

    Parameters
    ----------
    resolved : ScenarioInputs
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
        resolved.facilities_df, resolved.commodities_categories_df
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
