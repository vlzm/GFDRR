import pandas as pd

from .contracts import DataLoaderRebalancerProtocol, RebalancerConfig
from .routing.solver import Solver
from .routing.postprocessing import update_inventory_from_pdp


class Rebalancer:
    def __init__(
        self,
        dataloader_rebalancer: DataLoaderRebalancerProtocol,
        config: RebalancerConfig | dict,
    ):
        if isinstance(config, RebalancerConfig):
            self.config = config
        else:
            self.config = RebalancerConfig(**config)
        self.dataloader_rebalancer = dataloader_rebalancer

    def run(self, date: pd.Timestamp | None = None):
        """Run the full rebalancing pipeline: load → solve → postprocess."""

        # 1. Load graph snapshot, calculate demand, build PDP model
        self.dataloader_rebalancer.load_data(date=date)

        if self.dataloader_rebalancer.data is None:
            print("No imbalances to fix (either no sources or no destinations)")
            return

        # 2. Solve PDP
        solver = Solver(self.dataloader_rebalancer.data, self.config)
        self.route_df = solver.run()

        if self.route_df is None:
            return

        # 3. Postprocess: update inventory based on solution
        self.df_updated = update_inventory_from_pdp(
            self.dataloader_rebalancer.df_node_demand, self.route_df
        )
