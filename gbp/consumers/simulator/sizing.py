"""Sizing run: measure the initial inventory and dock capacities a scenario needs."""

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
    """Build artificial initial inventory holding ``quantity`` bikes per station."""
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
    """Smallest dock capacities that let the replay run with no dock-full/redirect."""
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
    """Smallest initial inventory and capacities for this config's demand."""
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
