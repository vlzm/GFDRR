## Solver
def solve_pdp(data: dict, time_limit_seconds: int = 30) -> dict | None:
    """Solve the Pickup and Delivery Problem."""
    manager = pywrapcp.RoutingIndexManager(
        len(data['distance_matrix']),
        data['num_resources'],
        data['depot']
    )
    routing = pywrapcp.RoutingModel(manager)

    # Distance callback
    def distance_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return data['distance_matrix'][from_node][to_node]

    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    # Capacity dimension
    def demand_callback(from_index):
        from_node = manager.IndexToNode(from_index)
        return data['demands'][from_node]

    demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(
        demand_callback_index,
        0,  # no slack
        data['resource_capacities'],
        True,  # start cumul at zero
        'Capacity'
    )

    # Pickup and Delivery constraints
    print('1')
    for pickup_idx, delivery_idx in data['pickups_deliveries']:
        pickup_index = manager.NodeToIndex(pickup_idx)
        delivery_index = manager.NodeToIndex(delivery_idx)
        
        # Same resource must handle both pickup and delivery
        routing.AddPickupAndDelivery(pickup_index, delivery_index)
        
        # Pickup must happen before delivery
        routing.solver().Add(
            routing.VehicleVar(pickup_index) == routing.VehicleVar(delivery_index)
        )
        
        # Time/distance precedence: pickup before delivery
        distance_dimension = routing.GetDimensionOrDie('Capacity')
        routing.solver().Add(
            distance_dimension.CumulVar(pickup_index) <= distance_dimension.CumulVar(delivery_index)
        )
    print('2')

    # Allow dropping nodes if infeasible (with high penalty)
    penalty = 100000
    print('3')
    for node in range(1, len(data['distance_matrix'])):
        routing.AddDisjunction([manager.NodeToIndex(node)], penalty)
    print('4')
    
    # Search parameters
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PARALLEL_CHEAPEST_INSERTION
    search_parameters.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    search_parameters.time_limit.seconds = time_limit_seconds

    solution = routing.SolveWithParameters(search_parameters)

    if not solution:
        return None

    return extract_pdp_solution(data, manager, routing, solution)