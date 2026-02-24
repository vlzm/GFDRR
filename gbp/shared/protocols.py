from typing import Protocol
import pandas as pd
import numpy as np

class DataLoaderGraphProtocol(Protocol):
    def prepare_stations_data(self, config: dict) -> pd.DataFrame:
        ...

    def prepare_depot_data(self, config: dict) -> pd.DataFrame:
        ...

    def prepare_resources_data(self, config: dict) -> pd.DataFrame:
        ...

    def load_data(self) -> None:
        ...

class DataLoaderRebalancerProtocol(Protocol):
    df_stations: pd.DataFrame
    df_depots: pd.DataFrame
    df_resources: pd.DataFrame

    def load_data(self) -> None:
        ...

    def load_rebalancer_data(self, pairs: list[dict], depot_coords: tuple, resource_capacity: int, num_resources: int) -> dict:
        ...

    def create_distance_matrix(self, locations: np.ndarray) -> np.ndarray:
        ...

    def create_pickup_delivery_pairs(self, sources: pd.DataFrame, destinations: pd.DataFrame) -> list[dict]:
        ...
