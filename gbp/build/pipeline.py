"""Orchestrate raw model validation, resolution, and derived artifacts."""

from __future__ import annotations

import dataclasses

import pandas as pd

from gbp.build.defaults import (
    default_commodity_categories,
    default_resource_categories,
    derive_demand_from_flow,
    derive_facility_roles,
    derive_inventory_from_flow,
    derive_inventory_initial,
    derive_supply_from_flow,
)
from gbp.build.edge_builder import build_edges
from gbp.build.time_resolution import (
    build_periods_from_segments,
    resolve_all_time_varying,
    resolve_registry_attributes,
)
from gbp.build.validation import validate_raw_model
from gbp.core.model import RawModelData, ResolvedModelData


def _prepare_distance_matrix(raw_dm: pd.DataFrame | None) -> pd.DataFrame | None:
    """Rename ``duration`` to ``lead_time_hours`` for ``build_edges`` compatibility.

    Parameters
    ----------
    raw_dm
        Distance matrix DataFrame or ``None``.

    Returns
    -------
    pd.DataFrame or None
        Renamed copy, or ``None`` if input is missing or empty.
    """
    if raw_dm is None or raw_dm.empty:
        return None
    return raw_dm.rename(columns={"duration": "lead_time_hours"})


def _ensure_edges_and_commodities(
    raw: RawModelData,
) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """Use ``raw.edges`` when populated; otherwise build from rules and distance_matrix.

    Parameters
    ----------
    raw
        Raw model data potentially missing explicit edges.

    Returns
    -------
    tuple of (pd.DataFrame or None, pd.DataFrame or None)
        ``(edges, edge_commodities)`` pair.
    """
    if raw.edges is not None and not raw.edges.empty:
        return raw.edges, raw.edge_commodities

    dm = _prepare_distance_matrix(raw.distance_matrix)
    manual = raw.scenario_manual_edges
    built = build_edges(raw.facilities, raw.edge_rules, manual, distance_matrix=dm)
    if built.empty:
        return raw.edges, raw.edge_commodities

    key_cols = ["source_id", "target_id", "modal_type"]
    if not all(c in built.columns for c in key_cols):
        return raw.edges, raw.edge_commodities

    grouped = built.groupby(key_cols, as_index=False).first()
    n = len(grouped)

    def series_or(name: str, default: float | str) -> pd.Series:
        if name in grouped.columns:
            return grouped[name]
        return pd.Series([default] * n, index=grouped.index)

    edges_df = pd.DataFrame(
        {
            "source_id": grouped["source_id"],
            "target_id": grouped["target_id"],
            "modal_type": grouped["modal_type"],
            "distance": series_or("distance", 0.0),
            "lead_time_hours": series_or("lead_time_hours", 24.0),
            "reliability": grouped["reliability"] if "reliability" in grouped.columns else None,
        }
    )

    if "commodity_category" not in built.columns:
        return edges_df, raw.edge_commodities

    ec = built[key_cols + ["commodity_category"]].drop_duplicates()
    ec = ec.assign(enabled=True)
    return edges_df, ec


def _apply_derivations(raw: RawModelData) -> RawModelData:
    """Fill in derivable tables on a copy of *raw*.

    Derivation is a strict no-op when the user-provided table is not ``None``.
    An explicitly empty DataFrame is treated as a user choice of "no rows" and
    is NOT re-derived -- only a missing (``None``) table triggers derivation.

    Parameters
    ----------
    raw
        Original raw model data.

    Returns
    -------
    RawModelData
        A copy with derived tables filled in, or the original if nothing
        was derived.
    """
    updates: dict[str, pd.DataFrame] = {}

    if raw.periods is None:
        updates["periods"] = build_periods_from_segments(
            raw.planning_horizon, raw.planning_horizon_segments,
        )

    if raw.commodity_categories is None:
        updates["commodity_categories"] = default_commodity_categories()

    if raw.resource_categories is None:
        has_resource_data = (
            (raw.resource_fleet is not None and not raw.resource_fleet.empty)
            or (raw.resources is not None and not raw.resources.empty)
        )
        if has_resource_data:
            updates["resource_categories"] = default_resource_categories()

    if raw.facility_roles is None:
        updates["facility_roles"] = derive_facility_roles(
            raw.facilities, raw.facility_operations,
        )

    if raw.demand is None and raw.observed_flow is not None and not raw.observed_flow.empty:
        updates["demand"] = derive_demand_from_flow(raw.observed_flow)

    if raw.supply is None and raw.observed_flow is not None and not raw.observed_flow.empty:
        updates["supply"] = derive_supply_from_flow(raw.observed_flow)

    if raw.inventory_initial is None:
        if raw.observed_inventory is not None and not raw.observed_inventory.empty:
            updates["inventory_initial"] = derive_inventory_initial(raw.observed_inventory)
        elif raw.observed_flow is not None and not raw.observed_flow.empty:
            seeded = derive_inventory_from_flow(raw.observed_flow)
            if not seeded.empty:
                updates["inventory_initial"] = seeded

    if not updates:
        return raw
    return dataclasses.replace(raw, **updates)


class BuildError(Exception):
    """Wrap an exception raised during a named build pipeline step.

    Parameters
    ----------
    step
        Name of the pipeline step that failed.
    cause
        Original exception.

    Attributes
    ----------
    step : str
        Name of the pipeline step that failed.
    cause : Exception
        Original exception.
    """

    def __init__(self, step: str, cause: Exception) -> None:
        self.step = step
        self.cause = cause
        super().__init__(f"build_model failed at step '{step}': {cause}")


def build_model(raw: RawModelData) -> ResolvedModelData:
    """Run the full build pipeline and return ``ResolvedModelData``.

    Before validation the pipeline fills in derivable tables that the user
    did not supply (periods, facility_roles, default categories, demand/supply
    from observed flow, inventory_initial from observed inventory).

    Parameters
    ----------
    raw
        Unresolved raw model data.

    Returns
    -------
    ResolvedModelData
        Fully resolved model ready for simulation.

    Raises
    ------
    ValueError
        If raw model validation fails.
    BuildError
        If any pipeline step raises an exception.
    """
    raw = _apply_derivations(raw)

    validate_raw_model(raw).raise_if_invalid()

    def _step(name: str, fn: object, *args: object, **kwargs: object) -> object:
        try:
            return fn(*args, **kwargs)  # type: ignore[operator]
        except Exception as exc:
            raise BuildError(name, exc) from exc

    periods = raw.periods.copy()
    resolved_time = _step("time_resolution", resolve_all_time_varying, raw, periods)

    resolved_attrs = _step(
        "registry_attributes", resolve_registry_attributes, raw.attributes, periods,
    )

    edges_df, ec_df = _step("edges", _ensure_edges_and_commodities, raw)

    return ResolvedModelData.from_raw(
        raw,
        periods=periods,
        resolved_time=resolved_time,
        resolved_attrs=resolved_attrs,
        edges=edges_df,
        edge_commodities=ec_df,
    )
