"""Facility and commodity hierarchy row schemas."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class FacilityHierarchyType(BaseModel):
    """One facility hierarchy kind (e.g. geographic, organizational)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    hierarchy_type_id: str
    name: str
    description: str | None = None


class FacilityHierarchyLevel(BaseModel):
    """Named level within a facility hierarchy."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    hierarchy_type_id: str
    level_index: int = Field(ge=0)
    level_name: str


class FacilityHierarchyNode(BaseModel):
    """Node in a facility hierarchy tree."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    node_id: str
    hierarchy_type_id: str
    level_index: int = Field(ge=0)
    parent_node_id: str | None = None
    name: str


class FacilityHierarchyMembership(BaseModel):
    """Maps a facility to one leaf node per hierarchy type."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    facility_id: str
    hierarchy_type_id: str
    node_id: str


class CommodityHierarchyType(BaseModel):
    """One commodity hierarchy kind (e.g. product_group)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    hierarchy_type_id: str
    name: str
    description: str | None = None


class CommodityHierarchyLevel(BaseModel):
    """Named level within a commodity hierarchy."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    hierarchy_type_id: str
    level_index: int = Field(ge=0)
    level_name: str


class CommodityHierarchyNode(BaseModel):
    """Node in a commodity hierarchy tree."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    node_id: str
    hierarchy_type_id: str
    level_index: int = Field(ge=0)
    parent_node_id: str | None = None
    name: str


class CommodityHierarchyMembership(BaseModel):
    """Maps a commodity category to one leaf node per hierarchy type."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    commodity_category_id: str
    hierarchy_type_id: str
    node_id: str
