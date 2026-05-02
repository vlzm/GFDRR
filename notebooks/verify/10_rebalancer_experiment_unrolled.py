import dataclasses
import math

import numpy as np
import pandas as pd
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

from gbp.build.pipeline import build_model
from gbp.core.enums import ModalType, ResourceStatus
from gbp.loaders import DataLoaderGraph, DataLoaderMock, GraphLoaderConfig


mock_config = {
    "n_stations": 10,
    "n_depots": 2,
    "n_timestamps": 72,
    "time_freq": "h",
    "start_date": "2025-01-01",
    "ebike_fraction": 0.3,
    "depot_capacity": 200,
    "seed": 42,
}
mock = DataLoaderMock(mock_config, n_trucks=3, truck_capacity_bikes=20)
loader = DataLoaderGraph(mock, GraphLoaderConfig())
raw = loader.load()
resolved_full = build_model(raw)
resolved = dataclasses.replace(resolved_full, supply=None)

demand_multiplier = 3.0
rebalance_every_n_periods = 6
rebalancer_commodity = "electric_bike"

min_threshold = 0.3
max_threshold = 0.7
time_limit_seconds = 5
pdp_random_seed = 42
truck_speed_kmh = 30.0
truck_resource_category = "rebalancing_truck"
modal_type_value = ModalType.ROAD.value


periods_df = resolved.periods
period_rows = [row._asdict() for row in periods_df.itertuples()]

if len(periods_df) >= 2:
    period_duration_hours = (
        pd.Timestamp(periods_df.iloc[1]["start_date"])
        - pd.Timestamp(periods_df.iloc[0]["start_date"])
    ).total_seconds() / 3600.0
else:
    period_duration_hours = 1.0


state_inventory = resolved.inventory_initial.copy()
state_in_transit = pd.DataFrame(
    columns=[
        "shipment_id",
        "source_id",
        "target_id",
        "commodity_category",
        "quantity",
        "resource_id",
        "departure_period",
        "arrival_period",
    ]
)
state_resources = resolved.resources.copy()
state_intermediates: dict[str, pd.DataFrame] = {}


flow_log_rows: list[pd.DataFrame] = []
unmet_demand_rows: list[pd.DataFrame] = []
rejected_dispatches_rows: list[pd.DataFrame] = []
latent_demand_log_rows: list[pd.DataFrame] = []
lost_demand_log_rows: list[pd.DataFrame] = []
period_log_rows: list[dict] = []


for period_cursor, period in enumerate(period_rows):
    period_id = period["period_id"]
    period_index = period["period_index"]


    flows_for_period = resolved.observed_flow[
        resolved.observed_flow["period_id"] == period_id
    ]

    if not flows_for_period.empty:
        latent_departures_df = (
            flows_for_period
            .groupby(["source_id", "commodity_category"], as_index=False)["quantity"]
            .sum()
            .rename(columns={"source_id": "facility_id", "quantity": "latent_departures"})
        )
        latent_arrivals_df = (
            flows_for_period
            .groupby(["target_id", "commodity_category"], as_index=False)["quantity"]
            .sum()
            .rename(columns={"target_id": "facility_id", "quantity": "latent_arrivals"})
        )
        latent_demand_df = latent_departures_df.merge(
            latent_arrivals_df, on=["facility_id", "commodity_category"], how="outer",
        )
        latent_demand_df["latent_departures"] = latent_demand_df["latent_departures"].fillna(0.0)
        latent_demand_df["latent_arrivals"] = latent_demand_df["latent_arrivals"].fillna(0.0)
        latent_demand_df = latent_demand_df.sort_values(
            ["facility_id", "commodity_category"], kind="stable",
        ).reset_index(drop=True)

        state_intermediates["latent_demand"] = latent_demand_df
        latent_demand_log_rows.append(
            latent_demand_df.assign(
                period_id=period_id,
                period_index=period_index,
                phase="HISTORICAL_LATENT_DEMAND",
            )
        )


    latent_demand_df = state_intermediates.get("latent_demand")
    if latent_demand_df is not None and not latent_demand_df.empty:
        latent_demand_df = latent_demand_df.copy()
        latent_demand_df["latent_departures"] = (
            latent_demand_df["latent_departures"] * demand_multiplier
        )
        latent_demand_df["latent_arrivals"] = (
            latent_demand_df["latent_arrivals"] * demand_multiplier
        )
        state_intermediates["latent_demand"] = latent_demand_df


    if not flows_for_period.empty:
        joint_df = (
            flows_for_period
            .groupby(["source_id", "target_id", "commodity_category"], as_index=False)["quantity"]
            .sum()
            .rename(columns={"quantity": "joint"})
        )
        origin_total_df = (
            joint_df
            .groupby(["source_id", "commodity_category"], as_index=False)["joint"]
            .sum()
            .rename(columns={"joint": "origin_total"})
        )
        od_probabilities_df = joint_df.merge(
            origin_total_df, on=["source_id", "commodity_category"], how="left",
        )
        od_probabilities_df["probability"] = (
            od_probabilities_df["joint"] / od_probabilities_df["origin_total"]
        )
        od_probabilities_df = od_probabilities_df[
            ["source_id", "target_id", "commodity_category", "probability"]
        ].sort_values(
            ["source_id", "commodity_category", "target_id"], kind="stable",
        ).reset_index(drop=True)

        state_intermediates["od_probabilities"] = od_probabilities_df


    latent_demand_df = state_intermediates.get("latent_demand")
    if latent_demand_df is not None and not latent_demand_df.empty:
        positive_departures_df = latent_demand_df.loc[
            latent_demand_df["latent_departures"] > 0,
            ["facility_id", "commodity_category", "latent_departures"],
        ]
        if not positive_departures_df.empty:
            inventory_with_demand = state_inventory.merge(
                positive_departures_df,
                on=["facility_id", "commodity_category"],
                how="outer",
            )
            inventory_with_demand["quantity"] = inventory_with_demand["quantity"].fillna(0.0)
            inventory_with_demand["latent_departures"] = (
                inventory_with_demand["latent_departures"].fillna(0.0)
            )
            available_quantity = inventory_with_demand["quantity"].clip(lower=0.0)
            inventory_with_demand["realized"] = np.minimum(
                available_quantity, inventory_with_demand["latent_departures"],
            )
            inventory_with_demand["lost"] = (
                inventory_with_demand["latent_departures"] - inventory_with_demand["realized"]
            )

            new_inventory = inventory_with_demand[["facility_id", "commodity_category"]].copy()
            new_inventory["quantity"] = (
                inventory_with_demand["quantity"] - inventory_with_demand["realized"]
            )
            state_inventory = new_inventory

            realized_departures_df = (
                inventory_with_demand.loc[
                    inventory_with_demand["realized"] > 0,
                    ["facility_id", "commodity_category", "realized"],
                ]
                .rename(columns={"realized": "realized_departures"})
                .reset_index(drop=True)
            )
            state_intermediates["realized_departures"] = realized_departures_df

            lost_rows = inventory_with_demand[inventory_with_demand["lost"] > 0]
            if not lost_rows.empty:
                lost_demand_log_rows.append(pd.DataFrame({
                    "facility_id": lost_rows["facility_id"].values,
                    "commodity_category": lost_rows["commodity_category"].values,
                    "latent": lost_rows["latent_departures"].values,
                    "realized": lost_rows["realized"].values,
                    "lost": lost_rows["lost"].values,
                    "period_id": period_id,
                    "period_index": period_index,
                    "phase": "DEPARTURE_PHYSICS",
                }))


    if not flows_for_period.empty:
        n_observed = len(flows_for_period)
        new_trips_df = pd.DataFrame({
            "shipment_id": [f"organic_trip_{period_index}_{i}" for i in range(n_observed)],
            "source_id": flows_for_period["source_id"].to_numpy(),
            "target_id": flows_for_period["target_id"].to_numpy(),
            "commodity_category": flows_for_period["commodity_category"].to_numpy(),
            "quantity": flows_for_period["quantity"].to_numpy(),
            "resource_id": [None] * n_observed,
            "departure_period": [period_index] * n_observed,
            "arrival_period": [period_index] * n_observed,
        })
        if state_in_transit.empty:
            state_in_transit = new_trips_df
        else:
            state_in_transit = pd.concat(
                [state_in_transit, new_trips_df], ignore_index=True,
            )


    if not state_in_transit.empty:
        arriving_mask = state_in_transit["arrival_period"] == period_index
        arriving_df = state_in_transit[arriving_mask]
        remaining_in_transit_df = state_in_transit[~arriving_mask].copy()

        if not arriving_df.empty:
            arrival_totals_df = (
                arriving_df
                .groupby(["target_id", "commodity_category"], as_index=False)["quantity"]
                .sum()
                .rename(columns={"target_id": "facility_id"})
            )
            inventory_with_arrivals = state_inventory.merge(
                arrival_totals_df,
                on=["facility_id", "commodity_category"],
                how="outer",
                suffixes=("", "_delta"),
            )
            inventory_with_arrivals["quantity"] = (
                inventory_with_arrivals["quantity"].fillna(0.0)
                + inventory_with_arrivals["quantity_delta"].fillna(0.0)
            )
            state_inventory = inventory_with_arrivals[
                ["facility_id", "commodity_category", "quantity"]
            ]

            arriving_resources_df = arriving_df[
                arriving_df["resource_id"].notna()
            ][["resource_id", "target_id"]].drop_duplicates(subset=["resource_id"])
            if not arriving_resources_df.empty:
                resource_to_target_map = arriving_resources_df.set_index("resource_id")["target_id"]
                resource_mask = state_resources["resource_id"].isin(resource_to_target_map.index)
                state_resources.loc[resource_mask, "status"] = ResourceStatus.AVAILABLE.value
                state_resources.loc[resource_mask, "available_at_period"] = None
                state_resources.loc[resource_mask, "current_facility_id"] = (
                    state_resources.loc[resource_mask, "resource_id"].map(resource_to_target_map)
                )

            modal_values = (
                arriving_df["modal_type"].values
                if "modal_type" in arriving_df.columns
                else [None] * len(arriving_df)
            )
            arrival_flow_events_df = pd.DataFrame({
                "source_id": arriving_df["source_id"].values,
                "target_id": arriving_df["target_id"].values,
                "commodity_category": arriving_df["commodity_category"].values,
                "modal_type": modal_values,
                "quantity": arriving_df["quantity"].values,
                "resource_id": arriving_df["resource_id"].values,
                "period_id": period_id,
                "period_index": period_index,
                "phase": "ARRIVALS",
            })
            flow_log_rows.append(arrival_flow_events_df)

            state_in_transit = remaining_in_transit_df


    dispatch_should_run = (period_index % rebalance_every_n_periods == 0)
    dispatches_df = pd.DataFrame(columns=[
        "source_id", "target_id", "commodity_category", "quantity",
        "resource_id", "modal_type", "arrival_period",
    ])

    if dispatch_should_run:
        available_trucks_df = state_resources[
            (state_resources["resource_category"] == truck_resource_category)
            & (state_resources["status"] == ResourceStatus.AVAILABLE.value)
        ].copy()

        bike_inventory_df = state_inventory[
            state_inventory["commodity_category"] == rebalancer_commodity
        ]
        station_facilities_df = resolved.facilities[
            resolved.facilities["facility_type"] == "station"
        ].copy()

        depot_facilities_df = resolved.facilities[
            resolved.facilities["facility_type"] == "depot"
        ].dropna(subset=["lat", "lon"])

        truck_capacity_match = resolved.resource_categories[
            resolved.resource_categories["resource_category_id"] == truck_resource_category
        ]
        truck_capacity = (
            float(truck_capacity_match.iloc[0]["base_capacity"])
            if not truck_capacity_match.empty else None
        )

        capacity_attribute = resolved.attributes.get("operation_capacity")
        capacity_attribute_data = (
            capacity_attribute.data if capacity_attribute is not None else None
        )
        if capacity_attribute_data is not None and not capacity_attribute_data.empty:
            storage_rows = capacity_attribute_data[
                capacity_attribute_data["operation_type"] == "storage"
            ]
            if "commodity_category" in storage_rows.columns:
                storage_rows = storage_rows[
                    storage_rows["commodity_category"] == rebalancer_commodity
                ]
            facility_capacity_series = (
                storage_rows.groupby("facility_id")["capacity"].sum().astype(float)
                if not storage_rows.empty else None
            )
        else:
            facility_capacity_series = None
        if facility_capacity_series is None or facility_capacity_series.empty:
            facility_capacity_series = (
                state_inventory.groupby("facility_id")["quantity"].max() * 2
            ).astype(float)

        rebalancer_can_run = (
            not available_trucks_df.empty
            and not bike_inventory_df.empty
            and not station_facilities_df.empty
            and not depot_facilities_df.empty
            and truck_capacity is not None
            and truck_capacity > 0
        )

        if rebalancer_can_run:
            station_quantities_df = bike_inventory_df.groupby(
                "facility_id", as_index=False,
            )["quantity"].sum()
            station_quantities_df = station_quantities_df[
                station_quantities_df["facility_id"].isin(station_facilities_df["facility_id"])
            ]

            node_state_df = station_quantities_df.merge(
                station_facilities_df[["facility_id", "lat", "lon"]],
                on="facility_id",
                how="left",
            ).rename(columns={
                "facility_id": "node_id",
                "lat": "latitude",
                "lon": "longitude",
            })
            node_state_df["inventory_capacity"] = (
                node_state_df["node_id"].map(facility_capacity_series).astype("float64")
            )
            node_state_df = node_state_df.dropna(
                subset=["latitude", "longitude", "inventory_capacity"],
            )
            node_state_df = node_state_df[node_state_df["inventory_capacity"] > 0].copy()
            node_state_df["quantity"] = node_state_df["quantity"].astype(int)
            node_state_df["inventory_capacity"] = node_state_df["inventory_capacity"].astype(int)
            node_state_df = node_state_df.reset_index(drop=True)

            if not node_state_df.empty:
                node_state_df["utilization"] = (
                    node_state_df["quantity"] / node_state_df["inventory_capacity"]
                )
                target_utilization = (min_threshold + max_threshold) / 2
                node_state_df["target_count"] = (
                    node_state_df["inventory_capacity"] * target_utilization
                )
                node_state_df["balance"] = (
                    node_state_df["quantity"] - node_state_df["target_count"]
                )

                sources_df = node_state_df[
                    (node_state_df["utilization"] > max_threshold)
                    & (node_state_df["balance"] > 1)
                ].copy()
                sources_df["excess"] = sources_df["balance"].astype(int)

                destinations_df = node_state_df[
                    (node_state_df["utilization"] < min_threshold)
                    & (node_state_df["balance"] < -1)
                ].copy()
                destinations_df["deficit"] = (-destinations_df["balance"]).astype(int)

                if not sources_df.empty and not destinations_df.empty:
                    depot_row = depot_facilities_df.iloc[0]
                    depot_id = str(depot_row["facility_id"])
                    depot_lat = float(depot_row["lat"])
                    depot_lon = float(depot_row["lon"])

                    supply_df = sources_df.sort_values(
                        "excess", ascending=False,
                    ).reset_index(drop=True).copy()
                    demand_df = destinations_df.sort_values(
                        "deficit", ascending=False,
                    ).reset_index(drop=True).copy()
                    supply_df["end"] = supply_df["excess"].cumsum()
                    supply_df["start"] = supply_df["end"] - supply_df["excess"]
                    demand_df["end"] = demand_df["deficit"].cumsum()
                    demand_df["start"] = demand_df["end"] - demand_df["deficit"]

                    cross_df = supply_df.assign(_k=1).merge(
                        demand_df.assign(_k=1), on="_k", suffixes=("_p", "_d"),
                    )
                    cross_df["quantity"] = (
                        cross_df[["end_p", "end_d"]].min(axis=1)
                        - cross_df[["start_p", "start_d"]].max(axis=1)
                    ).clip(lower=0).astype(int)
                    cross_df["quantity"] = cross_df["quantity"].clip(upper=int(truck_capacity))
                    cross_df = cross_df[cross_df["quantity"] > 0]

                    pickup_delivery_pairs: list[dict] = []
                    for _, pair_row in cross_df.iterrows():
                        pickup_delivery_pairs.append({
                            "pickup_node_id": str(pair_row["node_id_p"]),
                            "pickup_latitude": float(pair_row["latitude_p"]),
                            "pickup_longitude": float(pair_row["longitude_p"]),
                            "delivery_node_id": str(pair_row["node_id_d"]),
                            "delivery_latitude": float(pair_row["latitude_d"]),
                            "delivery_longitude": float(pair_row["longitude_d"]),
                            "quantity": int(pair_row["quantity"]),
                        })

                    if pickup_delivery_pairs:
                        n_trucks = len(available_trucks_df)
                        pdp_locations: list[tuple[float, float]] = [(depot_lat, depot_lon)]
                        pdp_graph_node_ids: list[str | None] = [depot_id]
                        pdp_node_ids: list[str] = ["depot"]
                        pdp_demands: list[int] = [0]
                        pdp_pickups_deliveries: list[tuple[int, int]] = []

                        for pair in pickup_delivery_pairs:
                            pickup_index_local = len(pdp_locations)
                            pdp_locations.append(
                                (pair["pickup_latitude"], pair["pickup_longitude"]),
                            )
                            pdp_graph_node_ids.append(pair["pickup_node_id"])
                            pdp_node_ids.append(f"{pair['pickup_node_id']}_pickup")
                            pdp_demands.append(pair["quantity"])

                            delivery_index_local = len(pdp_locations)
                            pdp_locations.append(
                                (pair["delivery_latitude"], pair["delivery_longitude"]),
                            )
                            pdp_graph_node_ids.append(pair["delivery_node_id"])
                            pdp_node_ids.append(f"{pair['delivery_node_id']}_delivery")
                            pdp_demands.append(-pair["quantity"])

                            pdp_pickups_deliveries.append(
                                (pickup_index_local, delivery_index_local),
                            )

                        edge_distance_map: dict[tuple[str, str], float] = {}
                        if (
                            resolved.distance_matrix is not None
                            and not resolved.distance_matrix.empty
                        ):
                            for _, distance_row in resolved.distance_matrix.iterrows():
                                edge_distance_map[(
                                    str(distance_row["source_id"]),
                                    str(distance_row["target_id"]),
                                )] = float(distance_row["distance"])

                        n_locations = len(pdp_locations)
                        distance_matrix_m = np.zeros((n_locations, n_locations), dtype=int)
                        for i in range(n_locations):
                            for j in range(n_locations):
                                if i == j:
                                    continue
                                source_node = pdp_graph_node_ids[i]
                                target_node = pdp_graph_node_ids[j]
                                edge_km = (
                                    edge_distance_map.get((source_node, target_node))
                                    if (source_node is not None and target_node is not None)
                                    else None
                                )
                                if edge_km is not None:
                                    distance_matrix_m[i, j] = int(round(edge_km * 1000.0))
                                else:
                                    lat1 = math.radians(pdp_locations[i][0])
                                    lon1 = math.radians(pdp_locations[i][1])
                                    lat2 = math.radians(pdp_locations[j][0])
                                    lon2 = math.radians(pdp_locations[j][1])
                                    haversine_arg = (
                                        math.sin((lat2 - lat1) / 2) ** 2
                                        + math.cos(lat1) * math.cos(lat2)
                                        * math.sin((lon2 - lon1) / 2) ** 2
                                    )
                                    distance_matrix_m[i, j] = int(round(
                                        6_371_000.0 * 2.0
                                        * math.atan2(
                                            math.sqrt(haversine_arg),
                                            math.sqrt(1.0 - haversine_arg),
                                        )
                                    ))

                        routing_manager = pywrapcp.RoutingIndexManager(
                            n_locations, n_trucks, 0,
                        )
                        routing_model = pywrapcp.RoutingModel(routing_manager)

                        def distance_callback(from_index, to_index):
                            return int(distance_matrix_m[
                                routing_manager.IndexToNode(from_index)
                            ][
                                routing_manager.IndexToNode(to_index)
                            ])

                        transit_callback_index = routing_model.RegisterTransitCallback(
                            distance_callback,
                        )
                        routing_model.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

                        def demand_callback(from_index):
                            return int(pdp_demands[routing_manager.IndexToNode(from_index)])

                        demand_callback_index = routing_model.RegisterUnaryTransitCallback(
                            demand_callback,
                        )
                        routing_model.AddDimensionWithVehicleCapacity(
                            demand_callback_index,
                            0,
                            [int(truck_capacity)] * n_trucks,
                            True,
                            "Capacity",
                        )

                        for pickup_local_idx, delivery_local_idx in pdp_pickups_deliveries:
                            pickup_index = routing_manager.NodeToIndex(pickup_local_idx)
                            delivery_index = routing_manager.NodeToIndex(delivery_local_idx)
                            routing_model.AddPickupAndDelivery(pickup_index, delivery_index)
                            routing_model.solver().Add(
                                routing_model.VehicleVar(pickup_index)
                                == routing_model.VehicleVar(delivery_index)
                            )
                            capacity_dimension = routing_model.GetDimensionOrDie("Capacity")
                            routing_model.solver().Add(
                                capacity_dimension.CumulVar(pickup_index)
                                <= capacity_dimension.CumulVar(delivery_index)
                            )

                        for unused_node in range(1, n_locations):
                            routing_model.AddDisjunction(
                                [routing_manager.NodeToIndex(unused_node)], 100_000,
                            )

                        search_parameters = pywrapcp.DefaultRoutingSearchParameters()
                        search_parameters.first_solution_strategy = (
                            routing_enums_pb2.FirstSolutionStrategy.PARALLEL_CHEAPEST_INSERTION
                        )
                        search_parameters.local_search_metaheuristic = (
                            routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
                        )
                        search_parameters.time_limit.seconds = int(time_limit_seconds)
                        search_parameters.random_seed = int(pdp_random_seed)

                        pdp_solution = routing_model.SolveWithParameters(search_parameters)

                        extracted_routes: list[dict] = []
                        if pdp_solution is not None:
                            for vehicle_id in range(n_trucks):
                                route_steps: list[dict] = []
                                route_distance_m = 0
                                route_load = 0
                                index = routing_model.Start(vehicle_id)
                                while not routing_model.IsEnd(index):
                                    node = routing_manager.IndexToNode(index)
                                    step_demand = int(pdp_demands[node])
                                    route_load += step_demand
                                    route_steps.append({
                                        "node_id": pdp_node_ids[node],
                                        "node_index": int(node),
                                        "demand": step_demand,
                                        "cumulative_load": route_load,
                                    })
                                    previous_index = index
                                    index = pdp_solution.Value(routing_model.NextVar(index))
                                    route_distance_m += routing_model.GetArcCostForVehicle(
                                        previous_index, index, vehicle_id,
                                    )
                                route_steps.append({
                                    "node_id": "depot",
                                    "node_index": 0,
                                    "demand": 0,
                                    "cumulative_load": route_load,
                                })
                                if len(route_steps) > 2:
                                    extracted_routes.append({
                                        "resource_id": int(vehicle_id),
                                        "route": route_steps,
                                        "distance": float(route_distance_m / 1000.0),
                                    })

                        if extracted_routes:
                            truck_ids_list = available_trucks_df["resource_id"].astype(str).tolist()
                            dispatch_rows: list[dict] = []
                            for route_info in extracted_routes:
                                truck_idx = int(route_info["resource_id"])
                                if truck_idx >= len(truck_ids_list):
                                    continue
                                truck_id_value = truck_ids_list[truck_idx]

                                pickup_quantities: dict[str, int] = {}
                                resolved_route_pairs: list[tuple[str, str, int]] = []
                                for step in route_info["route"]:
                                    raw_step_id = str(step["node_id"])
                                    step_demand = int(step["demand"])
                                    if raw_step_id == "depot":
                                        continue
                                    if "_pickup" in raw_step_id:
                                        pickup_node_id = raw_step_id.replace("_pickup", "")
                                        pickup_quantities[pickup_node_id] = abs(step_demand)
                                    elif "_delivery" in raw_step_id:
                                        delivery_node_id = raw_step_id.replace("_delivery", "")
                                        delivery_qty = abs(step_demand)
                                        for candidate_pair in pickup_delivery_pairs:
                                            if str(candidate_pair["delivery_node_id"]) != delivery_node_id:
                                                continue
                                            if int(candidate_pair["quantity"]) != delivery_qty:
                                                continue
                                            candidate_pickup_id = str(candidate_pair["pickup_node_id"])
                                            if candidate_pickup_id not in pickup_quantities:
                                                continue
                                            resolved_route_pairs.append((
                                                candidate_pickup_id,
                                                delivery_node_id,
                                                delivery_qty,
                                            ))
                                            break

                                if not resolved_route_pairs:
                                    continue

                                route_distance_km = float(route_info["distance"])
                                route_arrival_period = period_index + max(
                                    1,
                                    math.ceil(
                                        (route_distance_km / truck_speed_kmh)
                                        / period_duration_hours
                                    ),
                                )
                                for source_id_v, target_id_v, qty_v in resolved_route_pairs:
                                    dispatch_rows.append({
                                        "source_id": source_id_v,
                                        "target_id": target_id_v,
                                        "commodity_category": rebalancer_commodity,
                                        "quantity": int(qty_v),
                                        "resource_id": truck_id_value,
                                        "modal_type": modal_type_value,
                                        "arrival_period": int(route_arrival_period),
                                    })

                            if dispatch_rows:
                                dispatches_df = pd.DataFrame(
                                    dispatch_rows,
                                    columns=[
                                        "source_id", "target_id", "commodity_category",
                                        "quantity", "resource_id", "modal_type",
                                        "arrival_period",
                                    ],
                                )

        if not dispatches_df.empty:
            dispatches_df = dispatches_df.copy()
            dispatches_df["_reject_reason"] = None


            invalid_arrival_mask = dispatches_df["arrival_period"] < period_index
            dispatches_df.loc[
                invalid_arrival_mask & dispatches_df["_reject_reason"].isna(),
                "_reject_reason",
            ] = "invalid_arrival"


            if not resolved.edges.empty:
                edge_keys = set(zip(
                    resolved.edges["source_id"],
                    resolved.edges["target_id"],
                    resolved.edges["modal_type"],
                    strict=False,
                ))
                has_modal_mask = (
                    dispatches_df["modal_type"].notna()
                    & dispatches_df["_reject_reason"].isna()
                )
                if has_modal_mask.any():
                    sub_df = dispatches_df.loc[has_modal_mask]
                    bad_edge_series = pd.Series(
                        [
                            (s, t, m) not in edge_keys
                            for s, t, m in zip(
                                sub_df["source_id"],
                                sub_df["target_id"],
                                sub_df["modal_type"],
                                strict=False,
                            )
                        ],
                        index=sub_df.index,
                    )
                    dispatches_df.loc[
                        bad_edge_series[bad_edge_series].index,
                        "_reject_reason",
                    ] = "invalid_edge"


            has_resource_mask = (
                dispatches_df["resource_id"].notna()
                & dispatches_df["_reject_reason"].isna()
            )
            if has_resource_mask.any():
                available_resources_df = state_resources[
                    state_resources["status"] == ResourceStatus.AVAILABLE.value
                ]
                available_resource_ids = set(available_resources_df["resource_id"])
                available_at_source = set(zip(
                    available_resources_df["resource_id"],
                    available_resources_df["current_facility_id"],
                    strict=False,
                ))
                sub_df = dispatches_df.loc[has_resource_mask]
                bad_indices: list = []
                for resource_id_v, resource_group in sub_df.groupby(
                    "resource_id", sort=False,
                ):
                    if len(resource_group) == 1:
                        only_idx = resource_group.index[0]
                        only_source = resource_group["source_id"].iloc[0]
                        if (resource_id_v, only_source) not in available_at_source:
                            bad_indices.append(only_idx)
                        continue
                    if resource_id_v not in available_resource_ids:
                        bad_indices.extend(resource_group.index)
                if bad_indices:
                    dispatches_df.loc[bad_indices, "_reject_reason"] = "no_available_resource"


            has_resource_valid_mask = (
                dispatches_df["resource_id"].notna()
                & dispatches_df["_reject_reason"].isna()
            )
            if (
                has_resource_valid_mask.any()
                and not resolved.resource_categories.empty
            ):
                category_capacity_map = resolved.resource_categories.set_index(
                    "resource_category_id",
                )["base_capacity"]
                resource_category_map = state_resources.set_index(
                    "resource_id",
                )["resource_category"]

                sub_df = dispatches_df[has_resource_valid_mask].copy()
                sub_df["_res_cat"] = sub_df["resource_id"].map(resource_category_map)
                sub_df["_cap"] = sub_df["_res_cat"].map(category_capacity_map)
                used_per_resource = sub_df.groupby("resource_id")["quantity"].transform("sum")
                over_capacity_mask = used_per_resource > sub_df["_cap"]
                if over_capacity_mask.any():
                    over_idx = sub_df.index[over_capacity_mask]
                    not_yet_rejected = dispatches_df.loc[over_idx, "_reject_reason"].isna()
                    dispatches_df.loc[
                        not_yet_rejected[not_yet_rejected].index,
                        "_reject_reason",
                    ] = "over_capacity"


            pending_mask = dispatches_df["_reject_reason"].isna()
            if pending_mask.any():
                remaining_inventory: dict[tuple[str, str], float] = {}
                for _, inv_row in state_inventory.iterrows():
                    remaining_inventory[(
                        str(inv_row["facility_id"]),
                        str(inv_row["commodity_category"]),
                    )] = float(inv_row["quantity"])

                for idx in dispatches_df.index[pending_mask]:
                    dispatch_row = dispatches_df.loc[idx]
                    inv_key = (
                        str(dispatch_row["source_id"]),
                        str(dispatch_row["commodity_category"]),
                    )
                    available_qty = remaining_inventory.get(inv_key, 0.0)
                    requested_qty = float(dispatch_row["quantity"])
                    if requested_qty > available_qty:
                        dispatches_df.at[idx, "_reject_reason"] = "insufficient_inventory"
                    else:
                        remaining_inventory[inv_key] = available_qty - requested_qty


            rejected_mask = dispatches_df["_reject_reason"].notna()
            rejected_dispatches_df = dispatches_df[rejected_mask].rename(
                columns={"_reject_reason": "reason"},
            )
            valid_dispatches_df = dispatches_df[~rejected_mask].drop(
                columns=["_reject_reason"],
            )

            if not rejected_dispatches_df.empty:
                rejected_dispatches_rows.append(
                    rejected_dispatches_df.assign(
                        period_id=period_id,
                        period_index=period_index,
                        phase="DISPATCH_rebalancer",
                    )
                )


            if not valid_dispatches_df.empty:
                valid_dispatches_df = valid_dispatches_df.copy()
                valid_dispatches_df["shipment_id"] = [
                    f"shp_{period_index}_{i}" for i in range(len(valid_dispatches_df))
                ]
                valid_dispatches_df["departure_period"] = period_index

                outflow_totals_df = (
                    valid_dispatches_df
                    .groupby(["source_id", "commodity_category"], as_index=False)["quantity"]
                    .sum()
                    .rename(columns={"source_id": "facility_id"})
                )
                inventory_after_dispatch = state_inventory.merge(
                    outflow_totals_df,
                    on=["facility_id", "commodity_category"],
                    how="outer",
                    suffixes=("", "_delta"),
                )
                inventory_after_dispatch["quantity"] = (
                    inventory_after_dispatch["quantity"].fillna(0.0)
                    - inventory_after_dispatch["quantity_delta"].fillna(0.0)
                )
                state_inventory = inventory_after_dispatch[
                    ["facility_id", "commodity_category", "quantity"]
                ]

                new_shipments_df = valid_dispatches_df[[
                    "shipment_id", "source_id", "target_id", "commodity_category",
                    "quantity", "resource_id", "departure_period", "arrival_period",
                ]].copy()
                if state_in_transit.empty:
                    state_in_transit = new_shipments_df
                else:
                    state_in_transit = pd.concat(
                        [state_in_transit, new_shipments_df], ignore_index=True,
                    )

                dispatched_resources_df = valid_dispatches_df[
                    valid_dispatches_df["resource_id"].notna()
                ]
                if not dispatched_resources_df.empty:
                    truck_to_target_map = (
                        dispatched_resources_df
                        .drop_duplicates(subset=["resource_id"])
                        .set_index("resource_id")["target_id"]
                    )
                    truck_to_arrival_map = (
                        dispatched_resources_df
                        .drop_duplicates(subset=["resource_id"])
                        .set_index("resource_id")["arrival_period"]
                    )
                    truck_mask = state_resources["resource_id"].isin(truck_to_target_map.index)
                    state_resources.loc[truck_mask, "status"] = ResourceStatus.IN_TRANSIT.value
                    state_resources.loc[truck_mask, "current_facility_id"] = (
                        state_resources.loc[truck_mask, "resource_id"].map(truck_to_target_map)
                    )
                    state_resources.loc[truck_mask, "available_at_period"] = (
                        state_resources.loc[truck_mask, "resource_id"].map(truck_to_arrival_map)
                    )

                dispatch_flow_events_df = pd.DataFrame({
                    "source_id": valid_dispatches_df["source_id"].values,
                    "target_id": valid_dispatches_df["target_id"].values,
                    "commodity_category": valid_dispatches_df["commodity_category"].values,
                    "modal_type": valid_dispatches_df["modal_type"].values,
                    "quantity": valid_dispatches_df["quantity"].values,
                    "resource_id": valid_dispatches_df["resource_id"].values,
                    "period_id": period_id,
                    "period_index": period_index,
                    "phase": "DISPATCH_rebalancer",
                })
                flow_log_rows.append(dispatch_flow_events_df)


    period_log_rows.append({
        "period_id": period_id,
        "period_index": period_index,
        "total_inventory": float(state_inventory["quantity"].sum()),
        "in_transit_count": int(len(state_in_transit)),
        "available_trucks": int(
            (state_resources["status"] == ResourceStatus.AVAILABLE.value).sum()
        ),
    })


    if period_cursor + 1 < len(period_rows):
        state_intermediates = {}


simulation_flow_log = (
    pd.concat(flow_log_rows, ignore_index=True) if flow_log_rows else pd.DataFrame()
)
simulation_lost_demand_log = (
    pd.concat(lost_demand_log_rows, ignore_index=True)
    if lost_demand_log_rows else pd.DataFrame()
)
simulation_latent_demand_log = (
    pd.concat(latent_demand_log_rows, ignore_index=True)
    if latent_demand_log_rows else pd.DataFrame()
)
simulation_rejected_dispatches_log = (
    pd.concat(rejected_dispatches_rows, ignore_index=True)
    if rejected_dispatches_rows else pd.DataFrame()
)
simulation_period_log = pd.DataFrame(period_log_rows)
