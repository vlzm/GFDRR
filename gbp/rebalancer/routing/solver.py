import pandas as pd
from .postprocessing import update_inventory_from_pdp, format_pdp_route_output
from .vrp import solve_pdp

class Solver:
    """Solves the Pickup and Delivery Problem and postprocesses the result."""

    def __init__(self, data: dict, df_original: pd.DataFrame, config: dict):
        self.data = data
        self.df_original = df_original
        self.config = config

    def run(self) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
        """Solve PDP and return (route_df, df_updated) or (None, None)."""

        solution = solve_pdp(self.data, time_limit_seconds=self.config['time_limit_seconds'])

        if solution:
            print(
                f"Solution found — objective: {solution['objective']}, "
                f"total distance: {solution['total_distance']}, "
                f"dropped nodes: {solution['dropped_nodes']}"
            )
            route_df = format_pdp_route_output(solution, self.data['pairs'])
            df_updated = update_inventory_from_pdp(self.df_original, route_df)
            return route_df, df_updated

        print("No solution found within time limit")
        return None, None
