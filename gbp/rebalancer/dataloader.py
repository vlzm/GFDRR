from __future__ import annotations

import math

import numpy as np
import pandas as pd

from gbp.loaders.protocols import GraphLoaderProtocol

from .contracts import (
    DestinationsSchema,
    NodeStateSchema,
    PdpModel,
    RebalancerConfig,
    SourcesSchema,
)
from .demand import DemandCalculator


class DataLoaderRebalancer:
    """Build rebalancer data from a GraphData snapshot.

    Node types used for filtering are read from *config*:

    - ``inventory_node_type`` — nodes that carry rebalanceable inventory
    - ``depot_node_type``     — nodes that serve as VRP depot (resource origin)

    After ``load_data(date)`` the following attributes are available:

    - ``df_node_demand`` – inventory nodes with utilisation / demand columns
    - ``data``           – PDP model dict ready for the solver
                           (*None* when no imbalance detected)
    """

    def __init__(self, dataloader_graph: GraphLoaderProtocol, config: RebalancerConfig | dict):
        self.dataloader_graph = dataloader_graph
        if isinstance(config, RebalancerConfig):
            self.config = config
        else:
            self.config = RebalancerConfig(**config)

    def load_data(self, date: pd.Timestamp | None = None) -> None:
        if date is None:
            date = self.dataloader_graph.available_dates[0]

        snapshot = self.dataloader_graph.get_snapshot(date)

        inventory_type = self.config.inventory_node_type
        depot_type = self.config.depot_node_type

        # Input boundary: validate snapshot provides required data
        if snapshot.inventory is None:
            raise ValueError("Snapshot must include inventory data")
        if snapshot.coordinates is None:
            raise ValueError("Snapshot must include coordinates")
        if snapshot.resources is None:
            raise ValueError("Snapshot must include resources")
        if "inventory_capacity" not in snapshot.node_attributes:
            raise ValueError("Snapshot must include inventory_capacity node attribute")

        inv_nodes = snapshot.nodes[snapshot.nodes["node_type"] == inventory_type].copy()
        if len(inv_nodes) == 0:
            raise ValueError(
                f"No nodes of type '{inventory_type}' found in snapshot"
            )

        depot_nodes = snapshot.nodes[snapshot.nodes["node_type"] == depot_type].copy()
        if len(depot_nodes) == 0:
            raise ValueError(
                f"No nodes of type '{depot_type}' found in snapshot"
            )

        coordinates = snapshot.coordinates[["node_id", "latitude", "longitude"]].copy()
        capacities = snapshot.node_attributes["inventory_capacity"].data[
            ["node_id", "value"]
        ].rename(columns={"value": "inventory_capacity"})
        inventory = (
            snapshot.inventory[["node_id", "quantity"]]
            .groupby("node_id", as_index=False)["quantity"]
            .sum()
        )

        # Build node state by joining filtered nodes with coordinates, attributes, and inventory
        df_node_state = (
            inv_nodes[["id"]]
            .merge(
                coordinates,
                left_on="id",
                right_on="node_id",
                how="left",
            )
            .drop(columns=["id"])
            .merge(
                capacities,
                on="node_id",
                how="left",
            )
            .merge(
                inventory,
                on="node_id",
                how="left",
            )
        )
        df_node_state["quantity"] = df_node_state["quantity"].fillna(0).astype(int)
        if df_node_state["inventory_capacity"].isna().any():
            raise ValueError("Missing inventory_capacity for one or more inventory nodes")
        NodeStateSchema.validate(df_node_state)

        # Demand calculation
        demand_calculator = DemandCalculator(df_node_state, self.config)
        self.df_node_demand, sources, destinations = demand_calculator.calculate_demand()

        if len(sources) == 0 or len(destinations) == 0:
            self.data = None
            return

        # Depot coords (centroid of depot-type nodes)
        depot_coords_df = (
            depot_nodes[["id"]]
            .merge(
                coordinates,
                left_on="id",
                right_on="node_id",
                how="left",
            )
        )
        depot_coords = (
            float(depot_coords_df["latitude"].mean()),
            float(depot_coords_df["longitude"].mean()),
        )

        resource_capacities = snapshot.resources["capacity"].astype(int).tolist()
        if len(resource_capacities) == 0:
            raise ValueError("No resources available in snapshot")

        # PDP model
        pairs = self.create_pickup_delivery_pairs(sources, destinations)

        self.data = self._build_pdp_model(
            pairs=pairs,
            depot_coords=depot_coords,
            resource_capacities=resource_capacities,
            distance_service=snapshot.distance_service,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def create_distance_matrix(
        locations: list[tuple[float, float]],
        graph_node_ids: list[str | None],
        distance_service,
    ) -> np.ndarray:
        """Build distance matrix in metres using graph distance when possible."""
        n_locations = len(locations)
        matrix = np.zeros((n_locations, n_locations), dtype=int)

        for i in range(n_locations):
            for j in range(n_locations):
                if i == j:
                    continue

                source_node_id = graph_node_ids[i]
                target_node_id = graph_node_ids[j]
                if (
                    distance_service is not None and
                    source_node_id is not None and
                    target_node_id is not None
                ):
                    km = distance_service.get_distance(source_node_id, target_node_id)
                    matrix[i, j] = int(round(km * 1000))
                    continue

                matrix[i, j] = int(round(
                    DataLoaderRebalancer._haversine_distance_m(
                        locations[i], locations[j]
                    )
                ))

        return matrix

    @staticmethod
    def _haversine_distance_m(
        source_coords: tuple[float, float],
        target_coords: tuple[float, float],
    ) -> float:
        """Return great-circle distance between two points in meters."""
        source_lat, source_lon = source_coords
        target_lat, target_lon = target_coords

        lat1 = math.radians(source_lat)
        lon1 = math.radians(source_lon)
        lat2 = math.radians(target_lat)
        lon2 = math.radians(target_lon)

        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = (
            math.sin(dlat / 2) ** 2 +
            math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return 6_371_000 * c

    @staticmethod
    def create_pickup_delivery_pairs(
        sources: pd.DataFrame, destinations: pd.DataFrame,
    ) -> list[dict]:
        SourcesSchema.validate(sources)
        DestinationsSchema.validate(destinations)

        supply = sources.sort_values("excess", ascending=False).reset_index(drop=True)
        demand = destinations.sort_values("deficit", ascending=False).reset_index(drop=True)

        supply["end"] = supply["excess"].cumsum()
        supply["start"] = supply["end"] - supply["excess"]

        demand["end"] = demand["deficit"].cumsum()
        demand["start"] = demand["end"] - demand["deficit"]

        pairs = supply.assign(_k=1).merge(
            demand.assign(_k=1),
            on="_k",
            suffixes=("_p", "_d"),
        )

        pairs["quantity"] = (
            pairs[["end_p", "end_d"]].min(axis=1)
            - pairs[["start_p", "start_d"]].max(axis=1)
        ).clip(lower=0).astype(int)

        pairs = pairs.loc[pairs["quantity"] > 0, [
            "node_id_p", "latitude_p", "longitude_p",
            "node_id_d", "latitude_d", "longitude_d",
            "quantity",
        ]]
        pairs.columns = [
            "pickup_node_id", "pickup_latitude", "pickup_longitude",
            "delivery_node_id", "delivery_latitude", "delivery_longitude",
            "quantity",
        ]

        return pairs.to_dict("records")

    def _build_pdp_model(
        self,
        pairs: list[dict],
        depot_coords: tuple,
        resource_capacities: list[int],
        distance_service,
    ) -> PdpModel:
        """Node layout: [depot, pickup_1, delivery_1, pickup_2, delivery_2, ...]"""
        locations: list[tuple[float, float]] = [depot_coords]
        graph_node_ids: list[str | None] = [None]
        node_ids = ["depot"]
        demands = [0]
        pickups_deliveries = []

        for pair in pairs:
            pickup_idx = len(locations)
            locations.append((pair["pickup_latitude"], pair["pickup_longitude"]))
            graph_node_ids.append(pair["pickup_node_id"])
            node_ids.append(f"{pair['pickup_node_id']}_pickup")
            demands.append(pair["quantity"])

            delivery_idx = len(locations)
            locations.append((pair["delivery_latitude"], pair["delivery_longitude"]))
            graph_node_ids.append(pair["delivery_node_id"])
            node_ids.append(f"{pair['delivery_node_id']}_delivery")
            demands.append(-pair["quantity"])

            pickups_deliveries.append((pickup_idx, delivery_idx))

        return {
            "distance_matrix": self.create_distance_matrix(
                locations=locations,
                graph_node_ids=graph_node_ids,
                distance_service=distance_service,
            ),
            "demands": demands,
            "pickups_deliveries": pickups_deliveries,
            "resource_capacities": resource_capacities,
            "num_resources": len(resource_capacities),
            "depot": 0,
            "node_ids": node_ids,
            "pairs": pairs,
        }
