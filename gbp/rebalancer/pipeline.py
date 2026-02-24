import pandas as pd
import numpy as np
from .demand import DemandCalculator
from .routing.solver import Solver  
from ..shared.protocols import DataLoaderRebalancerProtocol

class Rebalancer:
    def __init__(self, dataloader_rebalancer: DataLoaderRebalancerProtocol, config: dict):
        self.config = config
        self.dataloader_rebalancer = dataloader_rebalancer

    def run(self):
        """Run the full rebalancing pipeline: load → demand → pairs → solve → postprocess."""

        # 1. Load graph data (stations, depots, resources)
        ###### Вот это должно быть в граф дataloader, а не в пайплайне. ######
        self.dataloader_rebalancer.load_data() # Вот этот метод должен использоваь граф дataloader, а не пайплайн. Пайплайн должен просто вызвать его и получить готовые датафреймы.
        ##########################################################################

        # 2. Calculate demand → identify sources and destinations and 3. Create pickup-delivery pairs and PDP data model
        ###### Вот это должно быть в rebalancer дataloader, а не в пайплайне. ######
        df_stations = self.dataloader_rebalancer.df_stations # Вместо этого, нужны функции, которые будут приготавливать эти данные с помощью графовых данных.
        df_depots = self.dataloader_rebalancer.df_depots # Вместо этого, нужны функции, которые будут приготавливать эти данные с помощью графовых данных.

        demand_calculator = DemandCalculator(df_stations, self.config)
        df_stations_demand, sources, destinations = demand_calculator.calculate_demand()

        if len(sources) == 0 or len(destinations) == 0:
            print("No imbalances to fix (either no sources or no destinations)")
            return

        pairs = self.dataloader_rebalancer.create_pickup_delivery_pairs(sources, destinations)
        depot_coords = (df_depots['lat'].mean(), df_depots['lon'].mean())

        data = self.dataloader_rebalancer.load_rebalancer_data(
            pairs=pairs,
            depot_coords=depot_coords,
            resource_capacity=self.config['resource_capacity'],
            num_resources=self.config['num_resources'],
        )
        ##########################################################################

        # 3. Solve and postprocess
        solver = Solver(data, df_stations_demand, self.config) # Вот здесь нужно убрать df_stations_demand. Он там только для постпроцессинга. А постпроцессинг должен быть отдельным шагом после решения. Пайплайн должен просто вызвать солвер и получить результат, а потом вызвать функцию постпроцессинга, которая обновит df_stations_demand на основе результата.
        self.route_df, self.df_updated = solver.run()
