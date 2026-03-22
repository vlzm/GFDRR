"""Enumerations for the graph-based logistics data model.

L2 enums are domain-agnostic. L3 enums in this package default to bike-sharing;
other domains may use string facility_type/operation_type in schemas instead.
"""

from __future__ import annotations

from enum import Enum


class FacilityRole(str, Enum):
    """Semantic behavior of a facility in the network flow."""

    SOURCE = "source"
    SINK = "sink"
    STORAGE = "storage"
    TRANSSHIPMENT = "transshipment"


class ModalType(str, Enum):
    """Transport modality for an edge (part of edge identity)."""

    ROAD = "road"
    RAIL = "rail"
    SEA = "sea"
    PIPELINE = "pipeline"
    AIR = "air"
    DIGITAL = "digital"


class PeriodType(str, Enum):
    """Granularity of a planning horizon segment."""

    DAY = "day"
    WEEK = "week"
    MONTH = "month"


class AttributeKind(str, Enum):
    """Semantic classification for numeric attributes."""

    COST = "cost"
    REVENUE = "revenue"
    RATE = "rate"
    CAPACITY = "capacity"
    ADDITIONAL = "additional"


class FacilityType(str, Enum):
    """Bike-sharing facility kinds (L3 default for this project)."""

    STATION = "station"
    DEPOT = "depot"
    MAINTENANCE_HUB = "maintenance_hub"


class OperationType(str, Enum):
    """Operations a facility may support (L3 default for bike-sharing)."""

    RECEIVING = "receiving"
    STORAGE = "storage"
    DISPATCH = "dispatch"
    HANDLING = "handling"
    REPAIR = "repair"
