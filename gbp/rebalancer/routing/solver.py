import pandas as pd

from .postprocessing import format_pdp_route_output
from .vrp import solve_pdp


class Solver:
    """Solves the Pickup and Delivery Problem (solve only, no inventory update)."""

    def __init__(self, data: dict, config: dict):
        self.data = data
        self.config = config

    def run(self) -> pd.DataFrame | None:
        """Solve PDP and return *route_df*, or *None* if no solution found."""
        solution = solve_pdp(self.data, time_limit_seconds=self.config['time_limit_seconds'])

        if solution:
            print(
                f"Solution found — objective: {solution['objective']}, "
                f"total distance: {solution['total_distance']}, "
                f"dropped nodes: {solution['dropped_nodes']}"
            )
            return format_pdp_route_output(solution, self.data['pairs'])

        print("No solution found within time limit")
        return None
