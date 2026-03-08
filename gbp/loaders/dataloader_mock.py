import numpy as np
import pandas as pd


class DataLoaderMock:
    """Generate temporal mock data for stations, depots, and resources.

    After ``load_data()`` the following attributes are available:

    Static:
        df_stations     – [node_id, inventory_capacity, lat, lon]
        df_depots       – [node_id, lat, lon]
        df_resources    – [resource_id, capacity]

    Temporal:
        timestamps      – DatetimeIndex
        df_inventory_ts – wide DataFrame (index=timestamps, columns=station node_ids,
                          values=commodity_quantity)
    """

    def __init__(self, config: dict):
        self.config = config

    def load_data(self) -> None:
        n_stations = self.config['n']
        n_depots = self.config.get('n_depots', 2)
        n_timestamps = self.config.get('n_timestamps', 168)
        start_date = self.config.get('start_date', '2025-01-01')
        freq = self.config.get('time_freq', 'h')

        self.df_stations = self._generate_stations(n_stations)
        self.df_depots = self._generate_depots(n_depots)
        self.df_resources = self._generate_resources()

        self.timestamps = pd.date_range(start=start_date, periods=n_timestamps, freq=freq)
        self.df_inventory_ts = self._generate_temporal_inventory(
            self.df_stations, self.timestamps
        )

    # ------------------------------------------------------------------

    @staticmethod
    def _generate_stations(n: int) -> pd.DataFrame:
        return pd.DataFrame({
            "node_id": [f"station_{i+1}" for i in range(n)],
            "inventory_capacity": np.random.randint(10, 50, size=n),
            "lat": np.random.uniform(40.7, 40.9, size=n),
            "lon": np.random.uniform(-74.0, -73.9, size=n),
        })

    @staticmethod
    def _generate_depots(n: int) -> pd.DataFrame:
        return pd.DataFrame({
            "node_id": [f"depot_{i+1}" for i in range(n)],
            "lat": np.random.uniform(40.7, 40.9, size=n),
            "lon": np.random.uniform(-74.0, -73.9, size=n),
        })

    def _generate_resources(self) -> pd.DataFrame:
        num = self.config.get('num_resources', 3)
        cap = self.config.get('resource_capacity', 100)
        return pd.DataFrame({
            "resource_id": [f"truck_{i+1}" for i in range(num)],
            "capacity": [cap] * num,
        })

    @staticmethod
    def _generate_temporal_inventory(
        df_stations: pd.DataFrame,
        timestamps: pd.DatetimeIndex,
    ) -> pd.DataFrame:
        """Random-walk inventory time-series (wide format)."""
        capacities = df_stations['inventory_capacity'].values
        n_t = len(timestamps)
        n_s = len(df_stations)

        base_qty = np.random.randint(0, capacities + 1)
        changes = np.random.randint(-3, 4, size=(n_t, n_s))
        quantities = base_qty + np.cumsum(changes, axis=0)
        quantities = np.clip(quantities, 0, capacities)

        return pd.DataFrame(
            quantities,
            index=timestamps,
            columns=df_stations['node_id'],
        )
