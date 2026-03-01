import numpy as np
import pandas as pd
from scipy.spatial.distance import cdist

from ..shared.decorators import validate
from ..shared.protocols import DataLoaderGraphProtocol
from ..shared.schemas import DestinationsSchema, NodeStateSchema, PdpModel, SourcesSchema
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

    def __init__(self, dataloader_graph: DataLoaderGraphProtocol, config: dict):
        self.config = config
        self.dataloader_graph = dataloader_graph

    def load_data(self, date: pd.Timestamp | None = None) -> None:
        if date is None:
            date = self.dataloader_graph.available_dates[0]

        snapshot = self.dataloader_graph.get_snapshot(date)

        inventory_type = self.config['inventory_node_type']
        depot_type = self.config['depot_node_type']

        # Input boundary: validate snapshot provides required data
        inventory = snapshot.inventory
        if inventory is None:
            raise ValueError("Snapshot must include inventory data")

        inv_nodes = snapshot.filter_nodes_by_type(inventory_type)
        if len(inv_nodes) == 0:
            raise ValueError(
                f"No nodes of type '{inventory_type}' found in snapshot"
            )

        depot_nodes = snapshot.filter_nodes_by_type(depot_type)
        if len(depot_nodes) == 0:
            raise ValueError(
                f"No nodes of type '{depot_type}' found in snapshot"
            )

        # Build node state by joining filtered nodes with inventory
        df_node_state = (
            inv_nodes[["node_id", "lat", "lon"]]
            .merge(
                inventory[["node_id", "commodity_quantity", "inventory_capacity"]],
                on="node_id",
            )
        )
        NodeStateSchema.validate(df_node_state)

        # Demand calculation
        demand_calculator = DemandCalculator(df_node_state, self.config)
        self.df_node_demand, sources, destinations = demand_calculator.calculate_demand()

        if len(sources) == 0 or len(destinations) == 0:
            self.data = None
            return

        # Depot coords (centroid of depot-type nodes)
        depot_coords = (depot_nodes['lat'].mean(), depot_nodes['lon'].mean())

        # PDP model
        pairs = self.create_pickup_delivery_pairs(sources, destinations)

        self.data = self._build_pdp_model(
            pairs=pairs,
            depot_coords=depot_coords,
            resource_capacity=self.config['resource_capacity'],
            num_resources=self.config['num_resources'],
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def create_distance_matrix(locations: np.ndarray) -> np.ndarray:
        """Approximate distance matrix in metres from lat/lon coordinates."""
        mean_lat = np.radians(locations[:, 0].mean())
        scaled = locations.copy()
        scaled[:, 1] *= np.cos(mean_lat)
        return (cdist(scaled, scaled, metric='euclidean') * 111_000).astype(int)

    @staticmethod
    @validate(inputs={"sources": SourcesSchema, "destinations": DestinationsSchema})
    def create_pickup_delivery_pairs(
        sources: pd.DataFrame, destinations: pd.DataFrame,
    ) -> list[dict]:
        supply = sources.sort_values('excess', ascending=False).reset_index(drop=True)
        demand = destinations.sort_values('deficit', ascending=False).reset_index(drop=True)

        supply['end'] = supply['excess'].cumsum()
        supply['start'] = supply['end'] - supply['excess']

        demand['end'] = demand['deficit'].cumsum()
        demand['start'] = demand['end'] - demand['deficit']

        pairs = supply.assign(_k=1).merge(demand.assign(_k=1), on='_k', suffixes=('_p', '_d'))

        pairs['quantity'] = (
            pairs[['end_p', 'end_d']].min(axis=1)
            - pairs[['start_p', 'start_d']].max(axis=1)
        ).clip(lower=0).astype(int)

        pairs = pairs.loc[pairs['quantity'] > 0, [
            'node_id_p', 'lat_p', 'lon_p',
            'node_id_d', 'lat_d', 'lon_d',
            'quantity',
        ]]
        pairs.columns = [
            'pickup_node_id', 'pickup_lat', 'pickup_lon',
            'delivery_node_id', 'delivery_lat', 'delivery_lon',
            'quantity',
        ]

        return pairs.to_dict('records')

    def _build_pdp_model(
        self,
        pairs: list[dict],
        depot_coords: tuple,
        resource_capacity: int,
        num_resources: int,
    ) -> PdpModel:
        """Node layout: [depot, pickup_1, delivery_1, pickup_2, delivery_2, ...]"""
        locations = [list(depot_coords)]
        node_ids = ['depot']
        demands = [0]
        pickups_deliveries = []

        for pair in pairs:
            pickup_idx = len(locations)
            locations.append([pair['pickup_lat'], pair['pickup_lon']])
            node_ids.append(f"{pair['pickup_node_id']}_pickup")
            demands.append(pair['quantity'])

            delivery_idx = len(locations)
            locations.append([pair['delivery_lat'], pair['delivery_lon']])
            node_ids.append(f"{pair['delivery_node_id']}_delivery")
            demands.append(-pair['quantity'])

            pickups_deliveries.append((pickup_idx, delivery_idx))

        return {
            'distance_matrix': self.create_distance_matrix(np.array(locations)),
            'demands': demands,
            'pickups_deliveries': pickups_deliveries,
            'resource_capacities': [resource_capacity] * num_resources,
            'num_resources': num_resources,
            'depot': 0,
            'node_ids': node_ids,
            'pairs': pairs,
        }
