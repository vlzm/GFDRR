import pandas as pd
import numpy as np
from .data_loader import DataLoader
from .demand import DemandCalculator
from .routing.solver import Solver

class Rebalancer:
    def __init__(self, config: dict):
        self.config = config
    
        self.data_loader = None
        self.df_stations = None
        self.df_original = None
        self.df_depots = None
        self.df_resources = None

        self.sources = None
        self.destinations = None

        self.route_df = None
        self.df_updated = None

    def load_data(self):
        self.data_loader = DataLoader(self.config)
        self.data_loader.load_data()
        self.df_stations = self.data_loader.df_stations
        self.df_original = self.df_stations.copy()
        self.df_depots = self.data_loader.df_depots
        self.df_resources = self.data_loader.df_resources

    def calculate_demand(self):
        demand_calculator = DemandCalculator(self.df_stations, self.config)
        return demand_calculator.calculate_demand()

    def run_solver(self):
        solver = Solver(self.sources, self.destinations, self.df_depots, self.df_original, self.config)
        self.route_df, self.df_updated = solver.run()

    def run(self):

        # Load data
        self.load_data()

        # Calculate demand
        self.df_stations, self.sources, self.destinations = self.calculate_demand()

        # Run solver
        if len(self.sources) > 0 and len(self.destinations) > 0:
            self.run_solver()
        else:
            print("No imbalances to fix (either no sources or no destinations)")