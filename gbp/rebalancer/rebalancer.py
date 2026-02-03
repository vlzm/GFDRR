"""
Generic Rebalancer

Works only with abstract/generic column names from the Graph Platform.
"""

import pandas as pd
import time
from typing import Dict, Any, Optional, List

from ...graph import GraphLoader
from .internals import RebalancerDataLoader, VRPSolver


class Rebalancer:
    """
    Generic rebalancer that works with abstract data structures.
    
    This class knows nothing about "scooters", "vans", or "parking spots".
    It only knows about:
    - nodes (with id, capacity, lat, lon)
    - resources (with id, capacity, home_location_id)
    - observations (with location_id, timestamp, product_count)
    - edges (with source_id, target_id, distance)
    """
    
    def __init__(self, config_path: str, time_limit: int = 60):
        """
        Initialize Rebalancer
        
        Args:
            config_path: Path to configuration YAML file
            time_limit: Maximum time for VRP solver in seconds
        """
        self.config_path = config_path
        self.time_limit = time_limit
        self.data: Optional[Dict[str, Any]] = None
        self.graph_loader: Optional[GraphLoader] = None
        self.data_loader: Optional[RebalancerDataLoader] = None
        
    def load_data(self) -> Dict[str, Any]:
        """
        Load data using GraphLoader
        
        Returns:
            Dictionary with generic data structures
        """
        print("\n" + "="*60)
        print("REBALANCER - Loading Data")
        print("="*60)
        
        self.graph_loader = GraphLoader(self.config_path)
        self.data = self.graph_loader.load()
        
        # Initialize data loader with graph data
        self.data_loader = RebalancerDataLoader(self.data)
        
        return self.data
    
    def find_imbalances(self, 
                        min_threshold: float = 0.3,
                        max_threshold: float = 0.8) -> pd.DataFrame:
        """
        Find locations with imbalance based on observations
        
        Uses RebalancerDataLoader to find imbalances.
        
        Args:
            min_threshold: Location is a source if utilization < min_threshold
            max_threshold: Location is a destination if utilization > max_threshold
            
        Returns:
            DataFrame with columns: id, lat, lon, balance
            (positive balance = surplus/source, negative = deficit/destination)
        """
        if self.data is None:
            self.load_data()
        
        if self.data_loader is None:
            self.data_loader = RebalancerDataLoader(self.data)
        
        return self.data_loader.find_imbalances(min_threshold, max_threshold)
    
    def create_vrp_data_model(self, 
                              imbalance_df: pd.DataFrame,
                              resource_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Create VRP data model from imbalance data
        
        Uses RebalancerDataLoader to convert graph data to VRP input format.
        
        Args:
            imbalance_df: DataFrame with imbalances (id, lat, lon, balance)
            resource_id: ID of resource to use (if None, uses first resource)
            
        Returns:
            VRP data model dictionary
        """
        if self.data is None:
            self.load_data()
        
        if self.data_loader is None:
            self.data_loader = RebalancerDataLoader(self.data)
        
        return self.data_loader.load(imbalance_df, resource_id)
    
    def solve(self, 
              min_threshold: float = 0.3,
              max_threshold: float = 0.8,
              resource_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Complete rebalancing pipeline
        
        Args:
            min_threshold: Minimum utilization threshold for sources
            max_threshold: Maximum utilization threshold for destinations
            resource_id: Resource ID to use (if None, uses first)
            
        Returns:
            Dictionary with solution details or None if no solution
        """
        print("\n" + "="*60)
        print("REBALANCER - Complete Pipeline")
        print("="*60)
        
        # Load data
        if self.data is None:
            self.load_data()
        
        # Find imbalances
        imbalance_df = self.find_imbalances(min_threshold, max_threshold)
        
        if len(imbalance_df) == 0:
            print("\nNo imbalances found. System is balanced!")
            return None
        
        # Create VRP data model
        vrp_data = self.create_vrp_data_model(imbalance_df, resource_id)
        
        # Solve VRP
        print("\n" + "="*60)
        print("SOLVING VRP")
        print("="*60)
        
        start_time = time.time()
        solver = VRPSolver(time_limit=self.time_limit)
        solution, routing, manager = solver.solve(vrp_data)
        end_time = time.time()
        print(f"\nVRP solved in {end_time - start_time:.2f} seconds")
        
        if solution:
            print("\nSolution found!")
            
            # Extract route
            route_data = self._extract_route(solution, routing, manager, vrp_data)
            
            return {
                'solution': solution,
                'routing': routing,
                'manager': manager,
                'route_data': route_data,
                'vrp_data': vrp_data
            }
        else:
            print("\n❌ No solution found")
            return None
    
    def _extract_route(self, solution, routing, manager, vrp_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract route from VRP solution
        
        Args:
            solution: OR-Tools solution
            routing: OR-Tools routing model
            manager: OR-Tools index manager
            vrp_data: VRP data model
            
        Returns:
            List of route steps with location info
        """
        route_steps = []
        
        locations = vrp_data['locations']
        distance_matrix = vrp_data['distance_matrix']
        demands = vrp_data['demands']
        
        index = routing.Start(0)
        route_distance = 0
        
        while not routing.IsEnd(index):
            node_index = manager.IndexToNode(index)
            location = locations.iloc[node_index]
            
            route_steps.append({
                'location_id': location['id'],
                'lat': location['lat'],
                'lon': location['lon'],
                'demand': demands[node_index],
                'action': 'pickup' if demands[node_index] < 0 else 'delivery' if demands[node_index] > 0 else 'depot'
            })
            
            previous_index = index
            index = solution.Value(routing.NextVar(index))
            
            if previous_index != routing.Start(0):
                from_node = manager.IndexToNode(previous_index)
                to_node = manager.IndexToNode(index)
                route_distance += distance_matrix[from_node][to_node] / 1000  # Convert back to km
        
        # Add final depot
        node_index = manager.IndexToNode(index)
        location = locations.iloc[node_index]
        route_steps.append({
            'location_id': location['id'],
            'lat': location['lat'],
            'lon': location['lon'],
            'demand': 0,
            'action': 'depot'
        })
        
        print(f"\nRoute extracted: {len(route_steps)} stops")
        print(f"Total distance: {route_distance:.2f} km")
        
        return route_steps
