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
    """Build PDP solver inputs from a ``GraphLoaderProtocol``.

    Extracts what the rebalancer needs directly from ``resolved`` model and
    the underlying ``source`` data — no intermediate snapshot format required.

    Node types used for filtering are read from *config*:

    - ``inventory_node_type`` — facility_type that carries rebalanceable inventory
    - ``depot_node_type``     — facility_type that serves as VRP depot

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

        res = self.dataloader_graph.resolved
        src = self.dataloader_graph.source

        inventory_type = self.config.inventory_node_type
        depot_type = self.config.depot_node_type

        facilities = res.facilities

        inv_facilities = facilities[facilities["facility_type"] == inventory_type].copy()
        if len(inv_facilities) == 0:
            raise ValueError(
                f"No facilities of type '{inventory_type}' found in resolved model"
            )

        depot_facilities = facilities[facilities["facility_type"] == depot_type].copy()
        if len(depot_facilities) == 0:
            raise ValueError(
                f"No facilities of type '{depot_type}' found in resolved model"
            )

        inv_ids = inv_facilities["facility_id"].astype(str).tolist()

        coordinates = inv_facilities[["facility_id", "lat", "lon"]].rename(
            columns={"facility_id": "node_id", "lat": "latitude", "lon": "longitude"},
        ).copy()

        if "operation_capacity" not in res.attributes:
            raise ValueError("No operation capacities found in resolved model")
        op_caps = res.attributes.get("operation_capacity").data
        if op_caps.empty:
            raise ValueError("No operation capacities found in resolved model")
        capacities = (
            op_caps[op_caps["operation_type"] == "storage"]
            .groupby("facility_id", as_index=False)["capacity"]
            .sum()
            .rename(columns={"facility_id": "node_id", "capacity": "inventory_capacity"})
        )

        inv_ts = src.df_inventory_ts
        if date not in inv_ts.index:
            nearest = inv_ts.index[inv_ts.index.get_indexer([date], method="nearest")[0]]
            date = pd.Timestamp(nearest)
        row = inv_ts.loc[date]
        totals = row.groupby(level="facility_id").sum()
        inventory = pd.DataFrame({
            "node_id": inv_ids,
            "quantity": [int(totals[sid]) for sid in inv_ids],
        })

        df_node_state = (
            coordinates
            .merge(capacities, on="node_id", how="left")
            .merge(inventory, on="node_id", how="left")
        )
        df_node_state["quantity"] = df_node_state["quantity"].fillna(0).astype(int)
        if df_node_state["inventory_capacity"].isna().any():
            raise ValueError("Missing inventory_capacity for one or more inventory nodes")
        NodeStateSchema.validate(df_node_state)

        demand_calculator = DemandCalculator(df_node_state, self.config)
        self.df_node_demand, sources, destinations = demand_calculator.calculate_demand()

        if len(sources) == 0 or len(destinations) == 0:
            self.data = None
            return

        depot_coords = (
            float(depot_facilities["lat"].mean()),
            float(depot_facilities["lon"].mean()),
        )

        resource_capacities = src.df_resource_capacities["capacity"].astype(int).tolist()
        if len(resource_capacities) == 0:
            raise ValueError("No resources available in source data")

        edge_distances = self._build_edge_distance_map(res.edges)

        pairs = self.create_pickup_delivery_pairs(sources, destinations)

        self.data = self._build_pdp_model(
            pairs=pairs,
            depot_coords=depot_coords,
            resource_capacities=resource_capacities,
            edge_distances=edge_distances,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_edge_distance_map(
        edges: pd.DataFrame | None,
    ) -> dict[tuple[str, str], float]:
        """Build {(source_id, target_id): distance_km} from resolved edges."""
        if edges is None or edges.empty:
            return {}
        return {
            (str(row["source_id"]), str(row["target_id"])): float(row["distance"])
            for _, row in edges.iterrows()
        }

    @staticmethod
    def create_distance_matrix(
        locations: list[tuple[float, float]],
        graph_node_ids: list[str | None],
        edge_distances: dict[tuple[str, str], float],
    ) -> np.ndarray:
        """Build distance matrix in metres; uses edge distances when available."""
        n_locations = len(locations)
        matrix = np.zeros((n_locations, n_locations), dtype=int)

        for i in range(n_locations):
            for j in range(n_locations):
                if i == j:
                    continue

                source_node_id = graph_node_ids[i]
                target_node_id = graph_node_ids[j]
                if source_node_id is not None and target_node_id is not None:
                    km = edge_distances.get((source_node_id, target_node_id))
                    if km is not None:
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
        edge_distances: dict[tuple[str, str], float],
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
                edge_distances=edge_distances,
            ),
            "demands": demands,
            "pickups_deliveries": pickups_deliveries,
            "resource_capacities": resource_capacities,
            "num_resources": len(resource_capacities),
            "depot": 0,
            "node_ids": node_ids,
            "pairs": pairs,
        }
