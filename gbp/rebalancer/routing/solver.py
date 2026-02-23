import numpy as np
import pandas as pd
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
from scipy.spatial.distance import cdist
from .postprocessing import update_inventory_from_pdp, format_pdp_route_output

class Solver:
    def __init__(self, sources, destinations, df_depots, df_original, config: dict):
        self.sources = sources
        self.destinations = destinations
        self.df_depots = df_depots
        self.df_original = df_original
        self.config = config

    # Move to dataloader
    def prepare_solver_data(self, sources, destinations):
        pairs = create_pickup_delivery_pairs(sources, destinations)
        depot_coords = (self.df_depots['lat'].mean(), self.df_depots['lon'].mean())
        data = create_pdp_data_model(
            pairs,
            depot_coords=depot_coords,
            resource_capacity=self.config['resource_capacity'],
            num_resources=self.config['num_resources']
        )
        return data, pairs
    
    def load_data(self):
        # Data loading is now handled in the pipeline, so this can be a no-op or used for any additional processing if needed
        pass
    
    def run_solver(self, data):
        solution = solve_pdp(data, time_limit_seconds=self.config['time_limit_seconds'])
        return solution

    def postprocess_solution(self, solution, pairs):
        route_df = format_pdp_route_output(solution, pairs)
        df_updated = update_inventory_from_pdp(self.df_original, route_df)
        return route_df, df_updated

    def run(self):

        # Move to pipeline
        data, pairs = self.prepare_solver_data(self.sources, self.destinations)

        # Load data
        self.load_data()    

        # Solve
        solution = self.run_solver(data)
        
        # Postprocess solution
        if solution:
            print(f"Solution found with objective {solution['objective']} and total distance {solution['total_distance']} (dropped nodes: {solution['dropped_nodes']})")
            route_df, df_updated = self.postprocess_solution(solution, pairs)
            return route_df, df_updated
        else:
            print("No solution found within time limit")
            print("No solution found")
            return None, None