import numpy as np
import pandas as pd
from scipy.spatial.distance import cdist
from ..shared.protocols import DataLoaderGraphProtocol

class DataLoaderRebalancer:
    def __init__(self, dataloader_graph: DataLoaderGraphProtocol, config: dict):
        self.config = config
        self.dataloader_graph = dataloader_graph

    def load_data(self):
        self.df_stations = self.dataloader_graph.prepare_stations_data(config=self.config)
        self.df_depots = self.dataloader_graph.prepare_depot_data(config=self.config)
        self.df_resources = self.dataloader_graph.prepare_resources_data(config=self.config)

    ## Move to dataloader
    def create_distance_matrix(self, locations: np.ndarray) -> np.ndarray:
        """Create distance matrix from lat/lon coordinates."""
        mean_lat = np.radians(locations[:, 0].mean())
        scaled_locations = locations.copy()
        scaled_locations[:, 1] *= np.cos(mean_lat)
        distances = cdist(scaled_locations, scaled_locations, metric='euclidean') * 111000
        return distances.astype(int)

    def create_pickup_delivery_pairs(self, sources: pd.DataFrame, destinations: pd.DataFrame) -> list[dict]:
        # Sort to preserve greedy "largest first" semantics
        supply = sources.sort_values('excess', ascending=False).reset_index(drop=True)
        demand = destinations.sort_values('deficit', ascending=False).reset_index(drop=True)
        
        # Build cumulative intervals: each source "owns" a segment of the number line
        supply['end'] = supply['excess'].cumsum()
        supply['start'] = supply['end'] - supply['excess']
        
        demand['end'] = demand['deficit'].cumsum()
        demand['start'] = demand['end'] - demand['deficit']
        
        # Cross join
        pairs = supply.assign(_k=1).merge(demand.assign(_k=1), on='_k', suffixes=('_p', '_d'))
        
        # Overlap = max(start_p, start_d) to min(end_p, end_d)
        pairs['quantity'] = (
            pairs[['end_p', 'end_d']].min(axis=1) - pairs[['start_p', 'start_d']].max(axis=1)
        ).clip(lower=0).astype(int)
        
        # Filter and rename
        pairs = pairs.loc[pairs['quantity'] > 0, [
            'node_id_p', 'lat_p', 'lon_p',
            'node_id_d', 'lat_d', 'lon_d', 
            'quantity'
        ]]
        pairs.columns = [
            'pickup_node_id', 'pickup_lat', 'pickup_lon',
            'delivery_node_id', 'delivery_lat', 'delivery_lon',
            'quantity'
        ]
        
        return pairs.to_dict('records')

    def load_rebalancer_data(
        self,
        pairs: list[dict],
        depot_coords: tuple,
        resource_capacity: int,
        num_resources: int
    ) -> dict:
        """
        Create data model for Pickup and Delivery Problem.
        
        Node layout: [depot, pickup_1, delivery_1, pickup_2, delivery_2, ...]
        """
        # Build locations array
        locations = [list(depot_coords)]  # depot at index 0
        node_ids = ['depot']
        demands = [0]
        pickups_deliveries = []
        
        for i, pair in enumerate(pairs):
            pickup_idx = len(locations)
            locations.append([pair['pickup_lat'], pair['pickup_lon']])
            node_ids.append(f"{pair['pickup_node_id']}_pickup")
            demands.append(pair['quantity'])  # Pickup increases load
            
            delivery_idx = len(locations)
            locations.append([pair['delivery_lat'], pair['delivery_lon']])
            node_ids.append(f"{pair['delivery_node_id']}_delivery")
            demands.append(-pair['quantity'])  # Delivery decreases load
            
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