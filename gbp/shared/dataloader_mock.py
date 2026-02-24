import numpy as np
import pandas as pd

class DataLoaderMock:
    """
    Data loader utility for generating fake stations, depots, and resources data.
    """
    def __init__(self, config: dict):
        self.config = config

    @staticmethod
    def prepare_stations_data(config: dict) -> pd.DataFrame:
        """
        Prepare fake stations data.

        Args:
            n: Number of stations to generate.
            min_threshold: Minimum threshold for utilization (unused here; present for API compatibility).
            max_threshold: Maximum threshold for utilization (unused here; present for API compatibility).

        Returns:
            DataFrame with columns: node_id, inventory_capacity, lat, lon, commodity_quantity
        """
        df_fake = pd.DataFrame({
            "node_id": [f"station_{i+1}" for i in range(config['n'])],
            "inventory_capacity": np.random.randint(10, 50, size=config['n']),
            "lat": np.random.uniform(40.7, 40.9, size=config['n']).tolist(),
            "lon": np.random.uniform(-74.0, -73.9, size=config['n']).tolist(),
            "commodity_quantity": np.random.randint(0, 40, size=config['n']).tolist()
        })
        return df_fake
    
    @staticmethod
    def prepare_depot_data(config: dict) -> pd.DataFrame:
        """
        Prepare depot data.

        Args:
            n: Number of depots to generate.

        Returns:
            DataFrame with columns: node_id, inventory_capacity, lat, lon, commodity_quantity
        """
        depot_data = {
            "node_id": [f"depot_{i+1}" for i in range(config['n'])],
            "inventory_capacity": [1000] * config['n'],
            "lat": np.random.uniform(40.7, 40.9, size=config['n']).tolist(),
            "lon": np.random.uniform(-74.0, -73.9, size=config['n']).tolist(),
            "commodity_quantity": np.random.randint(0, 40, size=config['n']).tolist()
        }
        return pd.DataFrame(depot_data)

    @staticmethod
    def prepare_resources_data(config: dict) -> pd.DataFrame:
        """
        Prepare resources (trucks) data.

        Args:
            n: Number of resources (trucks) to generate.

        Returns:
            DataFrame with columns: resource_id, capacity
        """
        resources_data = {
            "resource_id": [f"truck_{i+1}" for i in range(config['n'])],
            "capacity": [20] * config['n'],
        }
        return pd.DataFrame(resources_data)

    def load_data(self) -> None:
        self.df_stations = self.prepare_stations_data(self.config)
        self.df_depots = self.prepare_depot_data(self.config)
        self.df_resources = self.prepare_resources_data(self.config)