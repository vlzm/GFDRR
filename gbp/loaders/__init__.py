"""Data loaders: raw Citi Bike entities and the resolved graph model.

``RawModelData`` reads the trip CSV and GBFS feed into raw entity tables;
``ResolvedModelData`` builds the period grid, historical flow log and replay
demand the simulator consumes.
"""

from .dataloader_graph import ResolvedModelData, attach_simulation
from .dataloader_raw import RawModelData

__all__ = ["RawModelData", "ResolvedModelData", "attach_simulation"]
