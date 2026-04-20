"""Minimal bike-sharing ``RawModelData`` fixtures for build pipeline tests."""

from __future__ import annotations

from datetime import date

import pandas as pd

from gbp.core.attributes.defaults import register_bike_sharing_defaults
from gbp.core.enums import FacilityRole, PeriodType
from gbp.core.model import RawModelData


def minimal_raw_model(
    *,
    with_edges: bool = True,
    with_demand: bool = True,
    with_supply: bool = False,
    with_costs: bool = False,
    with_observations: bool = False,
) -> RawModelData:
    """Small network: depot d1, stations s1/s2, one commodity, 3 day periods."""
    facilities = pd.DataFrame(
        {
            "facility_id": ["d1", "s1", "s2"],
            "facility_type": ["depot", "station", "station"],
            "name": ["Depot", "Station 1", "Station 2"],
        }
    )
    commodity_categories = pd.DataFrame(
        {
            "commodity_category_id": ["working_bike"],
            "name": ["Working bike"],
            "unit": ["bike"],
        }
    )
    resource_categories = pd.DataFrame(
        {
            "resource_category_id": ["rebalancing_truck"],
            "name": ["Truck"],
            "base_capacity": [20.0],
        }
    )
    planning_horizon = pd.DataFrame(
        {
            "planning_horizon_id": ["h1"],
            "name": ["H1"],
            "start_date": [date(2025, 1, 1)],
            "end_date": [date(2025, 1, 4)],
        }
    )
    planning_horizon_segments = pd.DataFrame(
        {
            "planning_horizon_id": ["h1"],
            "segment_index": [0],
            "start_date": [date(2025, 1, 1)],
            "end_date": [date(2025, 1, 4)],
            "period_type": [PeriodType.DAY.value],
        }
    )
    periods = pd.DataFrame(
        {
            "period_id": ["p0", "p1", "p2"],
            "planning_horizon_id": ["h1", "h1", "h1"],
            "segment_index": [0, 0, 0],
            "period_index": [0, 1, 2],
            "period_type": [PeriodType.DAY.value] * 3,
            "start_date": [date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 3)],
            "end_date": [date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 4)],
        }
    )
    facility_roles = pd.DataFrame(
        {
            "facility_id": (
                ["d1", "d1", "s1", "s1", "s1", "s2", "s2", "s2"]
                if with_supply
                else ["d1", "d1", "s1", "s1", "s1", "s2", "s2"]
            ),
            "role": (
                [
                    FacilityRole.STORAGE.value,
                    FacilityRole.TRANSSHIPMENT.value,
                    FacilityRole.SOURCE.value,
                    FacilityRole.SINK.value,
                    FacilityRole.STORAGE.value,
                    FacilityRole.SINK.value,
                    FacilityRole.STORAGE.value,
                    FacilityRole.SOURCE.value,
                ]
                if with_supply
                else [
                    FacilityRole.STORAGE.value,
                    FacilityRole.TRANSSHIPMENT.value,
                    FacilityRole.SINK.value,
                    FacilityRole.STORAGE.value,
                    FacilityRole.SOURCE.value,
                    FacilityRole.SINK.value,
                    FacilityRole.STORAGE.value,
                ]
            ),
        }
    )
    facility_operations = pd.DataFrame(
        {
            "facility_id": ["d1", "d1", "d1", "s1", "s1", "s1", "s2", "s2", "s2"],
            "operation_type": [
                "receiving",
                "storage",
                "dispatch",
                "receiving",
                "storage",
                "dispatch",
                "receiving",
                "storage",
                "dispatch",
            ],
            "enabled": [True] * 9,
        }
    )
    edge_rules = pd.DataFrame(
        {
            "source_type": ["depot", "depot"],
            "target_type": ["station", "station"],
            "commodity_category": ["working_bike", "working_bike"],
            "modal_type": ["road", "road"],
            "enabled": [True, True],
        }
    )

    edge_commodities = None
    edges = None
    if with_edges:
        edges = pd.DataFrame(
            {
                "source_id": ["d1", "d1"],
                "target_id": ["s1", "s2"],
                "modal_type": ["road", "road"],
                "distance": [1.0, 2.0],
                "lead_time_hours": [24.0, 48.0],
                "reliability": [0.99, 0.99],
            }
        )
        edge_commodities = pd.DataFrame(
            {
                "source_id": ["d1", "d1"],
                "target_id": ["s1", "s2"],
                "modal_type": ["road", "road"],
                "commodity_category": ["working_bike", "working_bike"],
                "enabled": [True, True],
            }
        )

    demand = None
    if with_demand:
        demand = pd.DataFrame(
            {
                "facility_id": ["s1", "s1", "s2"],
                "commodity_category": ["working_bike"] * 3,
                "date": [date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 1)],
                "quantity": [1.0, 2.0, 3.0],
            }
        )

    supply = None
    if with_supply:
        supply = pd.DataFrame(
            {
                "facility_id": ["s1", "s1"],
                "commodity_category": ["working_bike", "working_bike"],
                "date": [date(2025, 1, 1), date(2025, 1, 2)],
                "quantity": [5.0, 5.0],
            }
        )

    resource_fleet = pd.DataFrame(
        {
            "facility_id": ["d1"],
            "resource_category": ["rebalancing_truck"],
            "count": [3],
        }
    )

    # ── cost DataFrames ──────────────────────────────────────────────
    operation_costs = None
    transport_costs = None
    resource_costs = None
    if with_costs:
        operation_costs = pd.DataFrame(
            {
                "facility_id": ["d1", "s1", "s1", "s2"],
                "operation_type": ["dispatch", "receiving", "receiving", "receiving"],
                "commodity_category": ["working_bike"] * 4,
                "date": [date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 1)],
                "cost_per_unit": [0.5, 1.0, 1.0, 1.5],
            }
        )
        transport_costs = pd.DataFrame(
            {
                "source_id": ["d1", "d1"],
                "target_id": ["s1", "s2"],
                "modal_type": ["road", "road"],
                "resource_category": ["rebalancing_truck", "rebalancing_truck"],
                "date": [date(2025, 1, 1), date(2025, 1, 1)],
                "cost_per_unit": [2.0, 3.5],
            }
        )
        resource_costs = pd.DataFrame(
            {
                "resource_category": ["rebalancing_truck", "rebalancing_truck"],
                "facility_id": [None, None],
                "attribute_name": ["fixed_cost_per_period", "maintenance_cost"],
                "date": [date(2025, 1, 1), date(2025, 1, 1)],
                "value": [10.0, 2.5],
            }
        )

    observed_flow = None
    observed_inventory = None
    if with_observations:
        observed_flow = pd.DataFrame(
            {
                "source_id": ["s1", "s1", "s2"],
                "target_id": ["s2", "s2", "s1"],
                "commodity_category": ["working_bike"] * 3,
                "date": [date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 1)],
                "quantity": [2.0, 3.0, 1.0],
            }
        )
        observed_inventory = pd.DataFrame(
            {
                "facility_id": ["s1", "s1", "s2"],
                "commodity_category": ["working_bike"] * 3,
                "date": [date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 1)],
                "quantity": [8.0, 6.0, 12.0],
            }
        )

    raw = RawModelData(
        facilities=facilities,
        commodity_categories=commodity_categories,
        resource_categories=resource_categories,
        planning_horizon=planning_horizon,
        planning_horizon_segments=planning_horizon_segments,
        periods=periods,
        facility_roles=facility_roles,
        facility_operations=facility_operations,
        edge_rules=edge_rules,
        edges=edges,
        edge_commodities=edge_commodities,
        demand=demand,
        supply=supply,
        observed_flow=observed_flow,
        observed_inventory=observed_inventory,
        resource_fleet=resource_fleet,
        resource_commodity_compatibility=pd.DataFrame(
            {
                "resource_category": ["rebalancing_truck"],
                "commodity_category": ["working_bike"],
                "enabled": [True],
            }
        ),
        resource_modal_compatibility=pd.DataFrame(
            {
                "resource_category": ["rebalancing_truck"],
                "modal_type": ["road"],
                "enabled": [True],
            }
        ),
    )

    if with_costs:
        register_bike_sharing_defaults(
            raw.attributes,
            operation_costs=operation_costs,
            transport_costs=transport_costs,
            resource_costs=resource_costs,
        )

    return raw
