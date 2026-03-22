"""N-to-M commodity transformation row schemas."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class Transformation(BaseModel):
    """One transformation process at a facility."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    transformation_id: str
    facility_id: str
    operation_type: str
    loss_rate: float = Field(ge=0, le=1)
    batch_size: float | None = Field(default=None, gt=0)
    batch_size_unit: str | None = None


class TransformationInput(BaseModel):
    """Input commodity ratio for one transformation cycle."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    transformation_id: str
    commodity_category: str
    ratio: float = Field(gt=0)


class TransformationOutput(BaseModel):
    """Output commodity ratio for one transformation cycle."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    transformation_id: str
    commodity_category: str
    ratio: float = Field(gt=0)
