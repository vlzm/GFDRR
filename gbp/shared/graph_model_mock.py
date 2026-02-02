"""Mock data loader for GraphData testing and development.

Provides utilities to generate realistic mock graph data for testing,
prototyping, and development without requiring real data sources.
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd

from gbp.shared.graph_model import (
    CommodityType,
    EdgeType,
    GraphData,
    NodeType,
)


@dataclass
class MockGraphConfig:
    """Configuration for mock graph generation.

    Attributes:
        n_depots: Number of depot nodes.
        edge_density: Probability of edge between any two nodes (0.0-1.0).
        include_demands: Whether to generate demands data.
        include_inventory: Whether to generate inventory data.
        center_lat: Center latitude for node generation.
        center_lon: Center longitude for node generation.
        spread: Geographic spread in degrees.
        seed: Random seed for reproducibility.
    """

    n_depots: int = 2
    edge_density: float = 0.3
    include_demands: bool = True
    include_inventory: bool = True
    center_lat: float = 55.75  # Moscow default
    center_lon: float = 37.62
    spread: float = 0.1
    seed: int | None = None


class GraphMockDataLoader:
    """Mock data loader for generating test GraphData instances.

    This class generates realistic mock data that conforms to the GraphData
    schema. Useful for testing, prototyping, and development.

    Example:
        >>> loader = GraphMockDataLoader(seed=42)
        >>> graph = loader.load()
        >>> print(graph)
        GraphData(nodes=32, edges=..., demands=..., inventory=...)

        >>> # Custom configuration
        >>> config = MockGraphConfig(n_depots=2, edge_density=0.5)
        >>> graph = loader.load(config)

        >>> # Preset scenarios
        >>> small_graph = loader.load_small()
        >>> large_graph = loader.load_large()
    """

    def __init__(self, seed: int | None = None) -> None:
        """Initialize mock data loader.

        Args:
            seed: Random seed for reproducibility. If None, results vary.
        """
        self._seed = seed
        self._rng = np.random.default_rng(seed)

    def load(self, config: MockGraphConfig | None = None) -> GraphData:
        """Generate mock GraphData based on configuration.

        Args:
            config: Generation configuration. Uses defaults if None.

        Returns:
            GraphData instance with mock data.
        """
        if config is None:
            config = MockGraphConfig(seed=self._seed)

        # Reset RNG if config has seed
        if config.seed is not None:
            self._rng = np.random.default_rng(config.seed)

        nodes = self._generate_nodes(config)
        edges = self._generate_edges(nodes, config)
        demands = self._generate_demands(nodes, config) if config.include_demands else None
        inventory = self._generate_inventory(nodes, config) if config.include_inventory else None

        return GraphData(
            nodes=nodes,
            edges=edges,
            demands=demands,
            inventory=inventory,
        )

    def load_small(self) -> GraphData:
        """Load a small mock graph for quick testing.

        Returns:
            GraphData with ~10 nodes.
        """
        config = MockGraphConfig(
            n_depots=2,
            edge_density=0.4,
            seed=self._seed,
        )
        return self.load(config)

    def load_medium(self) -> GraphData:
        """Load a medium-sized mock graph.

        Returns:
            GraphData with ~50 nodes.
        """
        config = MockGraphConfig(
            n_depots=3,
            edge_density=0.2,
            seed=self._seed,
        )
        return self.load(config)

    def load_large(self) -> GraphData:
        """Load a large mock graph for performance testing.

        Returns:
            GraphData with ~200 nodes.
        """
        config = MockGraphConfig(
            n_depots=10,
            edge_density=0.1,
            seed=self._seed,
        )
        return self.load(config)

    def load_minimal(self) -> GraphData:
        """Load minimal valid graph (2 nodes, 1 edge).

        Useful for unit testing edge cases.

        Returns:
            Minimal valid GraphData.
        """
        nodes = pd.DataFrame({
            "id": ["depot_0", "depot_1"],
            "node_type": [NodeType.DEPOT.value, NodeType.DEPOT.value],
            "node_name": ["Main Depot", "Secondary Depot"],
            "latitude": [55.75, 55.76],
            "longitude": [37.62, 37.63],
        })
        edges = pd.DataFrame({
            "source_id": ["depot_0"],
            "target_id": ["depot_1"],
            "edge_type": [EdgeType.TRANSFER.value],
            "distance": [1.5],
        })
        return GraphData(nodes=nodes, edges=edges)

    def _generate_nodes(self, config: MockGraphConfig) -> pd.DataFrame:
        """Generate nodes DataFrame.

        Args:
            config: Generation configuration.

        Returns:
            Nodes DataFrame.
        """
        nodes_data: list[dict] = []

        # Generate depots
        for i in range(config.n_depots):
            nodes_data.append({
                "id": f"depot_{i}",
                "node_type": NodeType.DEPOT.value,
                "node_name": f"Depot {chr(65 + i)}",  # A, B, C, ...
                "latitude": config.center_lat + self._rng.uniform(-config.spread, config.spread),
                "longitude": config.center_lon + self._rng.uniform(-config.spread, config.spread),
            })

        return pd.DataFrame(nodes_data)

    def _generate_edges(
        self, nodes: pd.DataFrame, config: MockGraphConfig
    ) -> pd.DataFrame:
        """Generate edges DataFrame with distances.

        Args:
            nodes: Nodes DataFrame.
            config: Generation configuration.

        Returns:
            Edges DataFrame.
        """
        node_ids = nodes["id"].tolist()
        coords = nodes.set_index("id")[["latitude", "longitude"]]
        node_types = nodes.set_index("id")["node_type"]
        edges_data: list[dict] = []

        for source_id in node_ids:
            for target_id in node_ids:
                if source_id == target_id:
                    continue

                # Random edge creation based on density
                if self._rng.random() > config.edge_density:
                    continue

                # Determine edge type based on node types
                source_type = node_types[source_id]
                target_type = node_types[target_id]

                if source_type == NodeType.DEPOT.value or target_type == NodeType.DEPOT.value:
                    edge_type = EdgeType.TRANSFER.value
                else:
                    edge_type = EdgeType.USAGE.value

                # Calculate haversine distance (simplified)
                lat1, lon1 = coords.loc[source_id]
                lat2, lon2 = coords.loc[target_id]
                distance = self._haversine_distance(lat1, lon1, lat2, lon2)

                edges_data.append({
                    "source_id": source_id,
                    "target_id": target_id,
                    "edge_type": edge_type,
                    "distance": round(distance, 2),
                })

        return pd.DataFrame(edges_data)

    def _generate_demands(
        self, nodes: pd.DataFrame, config: MockGraphConfig
    ) -> pd.DataFrame:
        """Generate demands DataFrame.

        Args:
            nodes: Nodes DataFrame.
            config: Generation configuration.

        Returns:
            Demands DataFrame.
        """
        demand_nodes = nodes[
            nodes["node_type"].isin([NodeType.DEPOT.value])
        ]["id"].tolist()

        demands_data = []
        for node_id in demand_nodes:
            # ~70% chance of having demand
            if self._rng.random() < 0.7:
                demands_data.append({
                    "node_id": node_id,
                    "commodity_type": CommodityType.SCOOTER.value,
                    "quantity": int(self._rng.integers(1, 10)),
                })

        return pd.DataFrame(demands_data) if demands_data else None

    def _generate_inventory(
        self, nodes: pd.DataFrame, config: MockGraphConfig
    ) -> pd.DataFrame:
        """Generate inventory DataFrame.

        Args:
            nodes: Nodes DataFrame.
            config: Generation configuration.

        Returns:
            Inventory DataFrame.
        """
        # Depots have inventory
        inventory_nodes = nodes[
            nodes["node_type"].isin([NodeType.DEPOT.value])
        ]["id"].tolist()

        inventory_data = []
        for node_id in inventory_nodes:
            node_type = nodes[nodes["id"] == node_id]["node_type"].iloc[0]

            # Depots have more inventory
            if node_type == NodeType.DEPOT.value:
                quantity = int(self._rng.integers(50, 200))
            else:
                quantity = int(self._rng.integers(0, 20))

            inventory_data.append({
                "node_id": node_id,
                "commodity_type": CommodityType.SCOOTER.value,
                "quantity": quantity,
            })

        return pd.DataFrame(inventory_data) if inventory_data else None

    @staticmethod
    def _haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate haversine distance between two points in kilometers.

        Args:
            lat1: Latitude of point 1.
            lon1: Longitude of point 1.
            lat2: Latitude of point 2.
            lon2: Longitude of point 2.

        Returns:
            Distance in kilometers.
        """
        r = 6371  # Earth radius in km

        lat1_rad = np.radians(lat1)
        lat2_rad = np.radians(lat2)
        delta_lat = np.radians(lat2 - lat1)
        delta_lon = np.radians(lon2 - lon1)

        a = (
            np.sin(delta_lat / 2) ** 2
            + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(delta_lon / 2) ** 2
        )
        c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

        return r * c
