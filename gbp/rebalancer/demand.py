import pandas as pd
import numpy as np

class DemandCalculator:
    def __init__(self, df_stations: pd.DataFrame, config: dict):
        self.df_stations = df_stations
        self.config = config

    def calculate_demand(self):
        return compute_utilization_and_balance(self.df_stations, self.config['min_threshold'], self.config['max_threshold'])
    
## Demand calculation
def compute_utilization_and_balance(df: pd.DataFrame, min_threshold: float, max_threshold: float) -> pd.DataFrame:
    """Compute utilization, target counts, and identify sources/destinations."""
    df = df.copy()
    df['utilization'] = df['commodity_quantity'] / df['inventory_capacity']
    target_utilization = (min_threshold + max_threshold) / 2
    df['target_count'] = df['inventory_capacity'] * target_utilization
    df['balance'] = df['commodity_quantity'] - df['target_count']

    sources = df[
        (df['utilization'] > max_threshold) &
        (df['balance'] > 1)
    ].copy()
    sources['excess'] = sources['balance'].astype(int)

    destinations = df[
        (df['utilization'] < min_threshold) &
        (df['balance'] < -1)
    ].copy()
    destinations['deficit'] = (-destinations['balance']).astype(int)

    return df, sources, destinations