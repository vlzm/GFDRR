"""Define scenario configuration row schemas."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class ScenarioManualEdges(BaseModel):
    """Represent manual edge triples added for a scenario."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    scenario_id: str
    source_id: str
    target_id: str
    modal_type: str
    commodity_category: str
