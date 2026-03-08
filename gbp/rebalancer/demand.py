import pandas as pd

from .contracts import (
    DestinationsSchema,
    NodeDemandSchema,
    NodeStateSchema,
    RebalancerConfig,
    SourcesSchema,
)


class DemandCalculator:
    def __init__(self, df_nodes: pd.DataFrame, config: RebalancerConfig):
        self.df_nodes = df_nodes
        self.config = config

    def calculate_demand(self):
        return compute_utilization_and_balance(
            self.df_nodes,
            self.config.min_threshold,
            self.config.max_threshold,
        )


def compute_utilization_and_balance(
    df: pd.DataFrame, min_threshold: float, max_threshold: float,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Compute utilization, target counts, and identify sources/destinations."""
    NodeStateSchema.validate(df)
    df = df.copy()
    df["utilization"] = df["quantity"] / df["inventory_capacity"]
    target_utilization = (min_threshold + max_threshold) / 2
    df["target_count"] = df["inventory_capacity"] * target_utilization
    df["balance"] = df["quantity"] - df["target_count"]

    sources = df[
        (df["utilization"] > max_threshold) &
        (df["balance"] > 1)
    ].copy()
    sources["excess"] = sources["balance"].astype(int)

    destinations = df[
        (df["utilization"] < min_threshold) &
        (df["balance"] < -1)
    ].copy()
    destinations["deficit"] = (-destinations["balance"]).astype(int)

    NodeDemandSchema.validate(df)
    SourcesSchema.validate(sources)
    DestinationsSchema.validate(destinations)

    return df, sources, destinations
