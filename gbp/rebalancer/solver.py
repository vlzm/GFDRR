"""
VRP Optimizer using OR-Tools
"""

from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
from typing import Tuple, Optional, Dict, Any


class VRPSolver:
    """Solves Vehicle Routing Problem using OR-Tools"""
    
    def __init__(self, time_limit: int = 60):
        """
        Initialize VRPSolver
        
        Args:
            time_limit: Maximum time for solver in seconds
        """
        self.time_limit = time_limit
    
    def solve(self, data: Dict[str, Any]) -> Tuple[Optional[Any], Any, Any]:
        """
        Solve the Vehicle Routing Problem using OR-Tools
        
        Args:
            data: Data model dictionary
            
        Returns:
            Tuple of (solution, routing, manager)
        """
        print("\n" + "="*60)
        print("Starting VRP optimization...")
        print("="*60)
        
        # Create routing index manager
        manager = pywrapcp.RoutingIndexManager(
            len(data['distance_matrix']),
            data['num_vehicles'],
            data['depot']
        )
        
        # Create routing model
        routing = pywrapcp.RoutingModel(manager)
        
        # Define distance callback
        def distance_callback(from_index, to_index):
            from_node = manager.IndexToNode(from_index)
            to_node = manager.IndexToNode(to_index)
            return data['distance_matrix'][from_node][to_node]
        
        transit_callback_index = routing.RegisterTransitCallback(distance_callback)
        routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)
        
        # Define capacity callback
        def demand_callback(from_index):
            from_node = manager.IndexToNode(from_index)
            return data['demands'][from_node]
        
        demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
        
        # Add capacity dimension with slack to allow for imbalanced system
        routing.AddDimensionWithVehicleCapacity(
            demand_callback_index,
            data['vehicle_capacity'],  # allow slack up to capacity
            [data['vehicle_capacity']] * data['num_vehicles'],
            True,  # start cumul to zero
            'Capacity'
        )
        
        # Allow dropping nodes if necessary (for unsolvable cases)
        penalty = 10000000  # High penalty for dropping nodes
        for node in range(1, len(data['distance_matrix'])):
            routing.AddDisjunction([manager.NodeToIndex(node)], penalty)
        
        # Set search parameters
        search_parameters = pywrapcp.DefaultRoutingSearchParameters()
        search_parameters.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
        )
        search_parameters.local_search_metaheuristic = (
            routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
        )
        search_parameters.time_limit.seconds = self.time_limit
        search_parameters.log_search = True
        
        # Solve
        print("Solving... (max {} seconds)".format(self.time_limit))
        print("Note: Solver may take time to find optimal route...")
        solution = routing.SolveWithParameters(search_parameters)
        
        return solution, routing, manager