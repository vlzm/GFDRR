"""Tests for rebalancer migration to GraphData API."""

from __future__ import annotations

import pandas as pd

from gbp.rebalancer import DataLoaderRebalancer, Rebalancer, RebalancerConfig
from gbp.rebalancer.demand import compute_utilization_and_balance


def _inject_imbalance(loaded_graph_loader, date: pd.Timestamp) -> None:
    """Create a deterministic imbalance for one timestamp."""
    stations = loaded_graph_loader._source.df_stations.reset_index(drop=True)

    for index, row in stations.iterrows():
        node_id = row["node_id"]
        capacity = int(row["inventory_capacity"])
        if index == 0:
            quantity = capacity
        elif index == 1:
            quantity = 0
        else:
            quantity = capacity // 2
        loaded_graph_loader._inventory_ts.loc[date, node_id] = quantity


def test_compute_utilization_and_balance_uses_quantity_column() -> None:
    df = pd.DataFrame({
        "node_id": ["station_1", "station_2"],
        "latitude": [40.75, 40.76],
        "longitude": [-73.98, -73.99],
        "quantity": [10, 1],
        "inventory_capacity": [10, 10],
    })

    node_demand, sources, destinations = compute_utilization_and_balance(
        df=df,
        min_threshold=0.3,
        max_threshold=0.7,
    )

    assert "utilization" in node_demand.columns
    assert "balance" in node_demand.columns
    assert set(sources["node_id"]) == {"station_1"}
    assert set(destinations["node_id"]) == {"station_2"}


def test_dataloader_rebalancer_builds_pdp_model(loaded_graph_loader) -> None:
    date = loaded_graph_loader.available_dates[0]
    _inject_imbalance(loaded_graph_loader, date)

    config = RebalancerConfig(
        inventory_node_type="station",
        depot_node_type="depot",
        min_threshold=0.3,
        max_threshold=0.7,
        time_limit_seconds=2,
    )
    dataloader = DataLoaderRebalancer(loaded_graph_loader, config)
    dataloader.load_data(date=date)

    assert dataloader.data is not None
    assert dataloader.data["num_resources"] > 0
    assert len(dataloader.data["distance_matrix"]) > 0
    assert "quantity" in dataloader.df_node_demand.columns


def test_rebalancer_pipeline_end_to_end(loaded_graph_loader) -> None:
    date = loaded_graph_loader.available_dates[0]
    _inject_imbalance(loaded_graph_loader, date)

    config = RebalancerConfig(
        inventory_node_type="station",
        depot_node_type="depot",
        min_threshold=0.3,
        max_threshold=0.7,
        time_limit_seconds=2,
    )
    dataloader = DataLoaderRebalancer(loaded_graph_loader, config)
    rebalancer = Rebalancer(dataloader, config)
    rebalancer.run(date=date)

    assert rebalancer.route_df is not None
    assert rebalancer.df_updated is not None
    assert "old_quantity" in rebalancer.df_updated.columns
    assert "inventory_change" in rebalancer.df_updated.columns
    assert "new_utilization" in rebalancer.df_updated.columns
