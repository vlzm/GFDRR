import pandas as pd


def extract_pdp_solution(data: dict, manager, routing, solution) -> dict:
    """Extract routes from PDP solution."""
    routes = []
    total_distance = 0
    dropped_nodes = []

    for node in range(1, len(data['distance_matrix'])):
        index = manager.NodeToIndex(node)
        if solution.Value(routing.NextVar(index)) == index:
            dropped_nodes.append(data['node_ids'][node])

    for resource_id in range(data['num_resources']):
        route = []
        route_distance = 0
        route_load = 0
        index = routing.Start(resource_id)

        while not routing.IsEnd(index):
            node = manager.IndexToNode(index)
            demand = data['demands'][node]
            route_load += demand

            route.append({
                'node_id': data['node_ids'][node],
                'node_index': node,
                'demand': demand,
                'cumulative_load': route_load,
            })

            previous_index = index
            index = solution.Value(routing.NextVar(index))
            route_distance += routing.GetArcCostForVehicle(previous_index, index, resource_id)

        route.append({
            'node_id': 'depot',
            'node_index': 0,
            'demand': 0,
            'cumulative_load': route_load,
        })

        if len(route) > 2:
            routes.append({
                'resource_id': resource_id,
                'route': route,
                'distance': route_distance,
            })
            total_distance += route_distance

    return {
        'routes': routes,
        'total_distance': total_distance,
        'objective': solution.ObjectiveValue(),
        'dropped_nodes': dropped_nodes,
    }


def format_pdp_route_output(solution: dict, pairs: list[dict]) -> pd.DataFrame:
    """Format PDP solution into DataFrame."""
    records = []
    for route_info in solution['routes']:
        for step, stop in enumerate(route_info['route']):
            raw_id = stop['node_id']
            if raw_id == 'depot':
                action = 'depot'
                node_id = 'depot'
            elif '_pickup' in raw_id:
                action = 'pickup'
                node_id = raw_id.replace('_pickup', '')
            else:
                action = 'delivery'
                node_id = raw_id.replace('_delivery', '')

            records.append({
                'resource_id': route_info['resource_id'],
                'step': step,
                'node_id': node_id,
                'action': action,
                'quantity': abs(stop['demand']),
                'cumulative_load': stop['cumulative_load'],
            })
    return pd.DataFrame(records)


def update_inventory_from_pdp(df_original: pd.DataFrame, route_df: pd.DataFrame) -> pd.DataFrame:
    """Update inventory based on PDP solution."""
    df_updated = df_original.copy()
    df_updated['old_commodity_quantity'] = df_updated['commodity_quantity']

    changes = route_df[route_df['action'] != 'depot'].copy()
    changes['delta'] = changes.apply(
        lambda row: -row['quantity'] if row['action'] == 'pickup' else row['quantity'],
        axis=1,
    )
    net_changes = changes.groupby('node_id')['delta'].sum()

    df_updated['inventory_change'] = df_updated['node_id'].map(net_changes).fillna(0).astype(int)
    df_updated['commodity_quantity'] = df_updated['commodity_quantity'] + df_updated['inventory_change']
    df_updated['new_utilization'] = df_updated['commodity_quantity'] / df_updated['inventory_capacity']

    return df_updated
