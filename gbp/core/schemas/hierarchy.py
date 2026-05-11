"""Define facility and commodity hierarchy row schemas."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class FacilityHierarchyType(BaseModel):
    """Represent one facility hierarchy kind (e.g. geographic, organizational)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    hierarchy_type_id: str
    name: str
    description: str | None = None


class FacilityHierarchyLevel(BaseModel):
    """Represent a named level within a facility hierarchy."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    hierarchy_type_id: str
    level_index: int = Field(ge=0)
    level_name: str


class FacilityHierarchyNode(BaseModel):
    """Represent a node in a facility hierarchy tree."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    node_id: str
    hierarchy_type_id: str
    level_index: int = Field(ge=0)
    parent_node_id: str | None = None
    name: str


class FacilityHierarchyMembership(BaseModel):
    """Map a facility to one leaf node per hierarchy type."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    facility_id: str
    hierarchy_type_id: str
    node_id: str


class CommodityHierarchyType(BaseModel):
    """Represent one commodity hierarchy kind (e.g. product_group)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    hierarchy_type_id: str
    name: str
    description: str | None = None


class CommodityHierarchyLevel(BaseModel):
    """Represent a named level within a commodity hierarchy."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    hierarchy_type_id: str
    level_index: int = Field(ge=0)
    level_name: str


class CommodityHierarchyNode(BaseModel):
    """Represent a node in a commodity hierarchy tree."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    node_id: str
    hierarchy_type_id: str
    level_index: int = Field(ge=0)
    parent_node_id: str | None = None
    name: str


class CommodityHierarchyMembership(BaseModel):
    """Map a commodity category to one leaf node per hierarchy type."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    commodity_category_id: str
    hierarchy_type_id: str
    node_id: str
