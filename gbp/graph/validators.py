"""Validation logic for GraphData.

This module provides comprehensive validation:
- Schema validation (required columns, types)
- Referential integrity (foreign keys)
- Business rule validation (custom checks)
"""

from dataclasses import dataclass
from typing import Callable

import pandas as pd

from gbp.graph.core import GraphData, AttributeTable, FlowsTable


# =============================================================================
# Validation Result
# =============================================================================


@dataclass
class ValidationError:
    """Single validation error."""
    
    level: str  # "error" or "warning"
    category: str  # "schema", "referential", "business"
    entity: str  # "nodes", "edge_attributes.transport_rate", etc.
    message: str
    
    def __str__(self) -> str:
        return f"[{self.level.upper()}] {self.category}/{self.entity}: {self.message}"


@dataclass
class ValidationResult:
    """Result of validation."""
    
    errors: list[ValidationError]
    
    @property
    def is_valid(self) -> bool:
        """True if no errors (warnings are ok)."""
        return not any(e.level == "error" for e in self.errors)
    
    @property
    def has_warnings(self) -> bool:
        """True if there are warnings."""
        return any(e.level == "warning" for e in self.errors)
    
    @property
    def error_count(self) -> int:
        return sum(1 for e in self.errors if e.level == "error")
    
    @property
    def warning_count(self) -> int:
        return sum(1 for e in self.errors if e.level == "warning")
    
    def __str__(self) -> str:
        if not self.errors:
            return "Validation passed: no errors or warnings"
        
        lines = [f"Validation result: {self.error_count} errors, {self.warning_count} warnings"]
        for error in self.errors:
            lines.append(f"  {error}")
        return "\n".join(lines)
    
    def raise_if_invalid(self) -> None:
        """Raise exception if validation failed."""
        if not self.is_valid:
            error_messages = [str(e) for e in self.errors if e.level == "error"]
            raise ValueError(
                f"Graph validation failed with {self.error_count} errors:\n" +
                "\n".join(error_messages)
            )


# =============================================================================
# Validator
# =============================================================================


class GraphValidator:
    """Validates GraphData for consistency and correctness.
    
    Performs three types of validation:
    1. Schema: Required columns exist, correct types
    2. Referential: Foreign keys reference existing entities
    3. Business: Custom rules (optional)
    
    Example:
        >>> validator = GraphValidator()
        >>> result = validator.validate(graph)
        >>> if not result.is_valid:
        ...     print(result)
        ...     result.raise_if_invalid()
    """
    
    def __init__(self) -> None:
        self._business_rules: list[Callable[[GraphData], list[ValidationError]]] = []
    
    def add_business_rule(
        self,
        rule: Callable[[GraphData], list[ValidationError]]
    ) -> None:
        """Add custom business rule.
        
        Args:
            rule: Function that takes GraphData and returns list of ValidationErrors.
        """
        self._business_rules.append(rule)
    
    def validate(self, graph: GraphData) -> ValidationResult:
        """Run all validations.
        
        Args:
            graph: GraphData to validate.
        
        Returns:
            ValidationResult with all errors and warnings.
        """
        errors: list[ValidationError] = []
        
        # Schema validation
        errors.extend(self._validate_nodes_schema(graph))
        errors.extend(self._validate_resources_schema(graph))
        errors.extend(self._validate_commodities_schema(graph))
        errors.extend(self._validate_coordinates_schema(graph))
        errors.extend(self._validate_attributes_schema(graph))
        errors.extend(self._validate_flows_schema(graph))
        errors.extend(self._validate_demands_schema(graph))
        errors.extend(self._validate_inventory_schema(graph))
        errors.extend(self._validate_telemetry_schema(graph))
        errors.extend(self._validate_tags_schema(graph))
        
        # Referential integrity
        errors.extend(self._validate_referential_integrity(graph))
        
        # Business rules
        for rule in self._business_rules:
            errors.extend(rule(graph))
        
        return ValidationResult(errors=errors)
    
    # =========================================================================
    # Schema Validation
    # =========================================================================
    
    def _validate_nodes_schema(self, graph: GraphData) -> list[ValidationError]:
        """Validate nodes DataFrame schema."""
        errors = []
        
        required_cols = {"id", "node_type"}
        missing = required_cols - set(graph.nodes.columns)
        if missing:
            errors.append(ValidationError(
                level="error",
                category="schema",
                entity="nodes",
                message=f"Missing required columns: {missing}"
            ))
        
        if "id" in graph.nodes.columns:
            if graph.nodes["id"].isna().any():
                errors.append(ValidationError(
                    level="error",
                    category="schema",
                    entity="nodes",
                    message="Column 'id' contains null values"
                ))
            
            if graph.nodes["id"].duplicated().any():
                dups = graph.nodes[graph.nodes["id"].duplicated()]["id"].tolist()
                errors.append(ValidationError(
                    level="error",
                    category="schema",
                    entity="nodes",
                    message=f"Duplicate node IDs: {dups[:5]}{'...' if len(dups) > 5 else ''}"
                ))
        
        return errors
    
    def _validate_resources_schema(self, graph: GraphData) -> list[ValidationError]:
        """Validate resources DataFrame schema."""
        errors = []
        
        if graph.resources is None:
            return errors
        
        required_cols = {"id", "resource_type"}
        missing = required_cols - set(graph.resources.columns)
        if missing:
            errors.append(ValidationError(
                level="error",
                category="schema",
                entity="resources",
                message=f"Missing required columns: {missing}"
            ))
        
        return errors
    
    def _validate_commodities_schema(self, graph: GraphData) -> list[ValidationError]:
        """Validate commodities DataFrame schema."""
        errors = []
        
        if graph.commodities is None:
            return errors
        
        required_cols = {"id", "commodity_type"}
        missing = required_cols - set(graph.commodities.columns)
        if missing:
            errors.append(ValidationError(
                level="error",
                category="schema",
                entity="commodities",
                message=f"Missing required columns: {missing}"
            ))
        
        return errors
    
    def _validate_coordinates_schema(self, graph: GraphData) -> list[ValidationError]:
        """Validate coordinates DataFrame schema."""
        errors = []
        
        if graph.coordinates is None:
            return errors
        
        required_cols = {"node_id", "latitude", "longitude"}
        missing = required_cols - set(graph.coordinates.columns)
        if missing:
            errors.append(ValidationError(
                level="error",
                category="schema",
                entity="coordinates",
                message=f"Missing required columns: {missing}"
            ))
        
        # Check latitude range
        if "latitude" in graph.coordinates.columns:
            invalid = (graph.coordinates["latitude"] < -90) | (graph.coordinates["latitude"] > 90)
            if invalid.any():
                errors.append(ValidationError(
                    level="error",
                    category="schema",
                    entity="coordinates",
                    message=f"Latitude values out of range [-90, 90]: {invalid.sum()} rows"
                ))
        
        # Check longitude range
        if "longitude" in graph.coordinates.columns:
            invalid = (graph.coordinates["longitude"] < -180) | (graph.coordinates["longitude"] > 180)
            if invalid.any():
                errors.append(ValidationError(
                    level="error",
                    category="schema",
                    entity="coordinates",
                    message=f"Longitude values out of range [-180, 180]: {invalid.sum()} rows"
                ))
        
        return errors
    
    def _validate_attributes_schema(self, graph: GraphData) -> list[ValidationError]:
        """Validate attribute tables."""
        errors = []
        
        # Node attributes
        for name, attr in graph.node_attributes.items():
            errors.extend(self._validate_single_attribute(attr, f"node_attributes.{name}"))
        
        # Edge attributes
        for name, attr in graph.edge_attributes.items():
            errors.extend(self._validate_single_attribute(attr, f"edge_attributes.{name}"))
        
        return errors
    
    def _validate_single_attribute(
        self,
        attr: AttributeTable,
        entity_name: str
    ) -> list[ValidationError]:
        """Validate single AttributeTable."""
        errors = []
        
        # Check all declared columns exist
        all_cols = set(attr.granularity_keys) | set(attr.value_columns)
        missing = all_cols - set(attr.data.columns)
        if missing:
            errors.append(ValidationError(
                level="error",
                category="schema",
                entity=entity_name,
                message=f"Missing columns: {missing}"
            ))
        
        # Check entity keys
        if attr.entity_type == "node":
            if "node_id" not in attr.granularity_keys:
                errors.append(ValidationError(
                    level="error",
                    category="schema",
                    entity=entity_name,
                    message="Node attribute must have 'node_id' in granularity_keys"
                ))
        elif attr.entity_type == "edge":
            if "source_id" not in attr.granularity_keys or "target_id" not in attr.granularity_keys:
                errors.append(ValidationError(
                    level="error",
                    category="schema",
                    entity=entity_name,
                    message="Edge attribute must have 'source_id' and 'target_id' in granularity_keys"
                ))
        
        # Check for null values in granularity keys
        for key in attr.granularity_keys:
            if key in attr.data.columns and attr.data[key].isna().any():
                errors.append(ValidationError(
                    level="warning",
                    category="schema",
                    entity=entity_name,
                    message=f"Granularity key '{key}' contains null values"
                ))
        
        return errors
    
    def _validate_flows_schema(self, graph: GraphData) -> list[ValidationError]:
        """Validate flows tables."""
        errors = []
        
        for name, flow in graph.flows.items():
            entity_name = f"flows.{name}"
            
            # Check source_id, target_id
            if "source_id" not in flow.granularity_keys:
                errors.append(ValidationError(
                    level="error",
                    category="schema",
                    entity=entity_name,
                    message="Flows must have 'source_id' in granularity_keys"
                ))
            
            if "target_id" not in flow.granularity_keys:
                errors.append(ValidationError(
                    level="error",
                    category="schema",
                    entity=entity_name,
                    message="Flows must have 'target_id' in granularity_keys"
                ))
            
            # Check value column exists
            if flow.value_column not in flow.data.columns:
                errors.append(ValidationError(
                    level="error",
                    category="schema",
                    entity=entity_name,
                    message=f"Value column '{flow.value_column}' not found"
                ))
        
        return errors
    
    def _validate_demands_schema(self, graph: GraphData) -> list[ValidationError]:
        """Validate demands DataFrame schema."""
        errors = []
        
        if graph.demands is None:
            return errors
        
        required_cols = {"node_id", "commodity_id", "period", "quantity"}
        missing = required_cols - set(graph.demands.columns)
        if missing:
            errors.append(ValidationError(
                level="error",
                category="schema",
                entity="demands",
                message=f"Missing required columns: {missing}"
            ))
        
        return errors
    
    def _validate_inventory_schema(self, graph: GraphData) -> list[ValidationError]:
        """Validate inventory DataFrame schema."""
        errors = []
        
        if graph.inventory is None:
            return errors
        
        required_cols = {"node_id", "commodity_id", "quantity"}
        missing = required_cols - set(graph.inventory.columns)
        if missing:
            errors.append(ValidationError(
                level="error",
                category="schema",
                entity="inventory",
                message=f"Missing required columns: {missing}"
            ))
        
        return errors
    
    def _validate_telemetry_schema(self, graph: GraphData) -> list[ValidationError]:
        """Validate telemetry DataFrame schema."""
        errors = []
        
        if graph.telemetry is None:
            return errors
        
        required_cols = {"node_id", "metric", "timestamp", "value"}
        missing = required_cols - set(graph.telemetry.columns)
        if missing:
            errors.append(ValidationError(
                level="error",
                category="schema",
                entity="telemetry",
                message=f"Missing required columns: {missing}"
            ))
        
        return errors
    
    def _validate_tags_schema(self, graph: GraphData) -> list[ValidationError]:
        """Validate tags DataFrame schema."""
        errors = []
        
        if graph.tags is None:
            return errors
        
        required_cols = {"entity_type", "entity_id", "key", "value"}
        missing = required_cols - set(graph.tags.columns)
        if missing:
            errors.append(ValidationError(
                level="error",
                category="schema",
                entity="tags",
                message=f"Missing required columns: {missing}"
            ))
        
        # Check entity_type values
        if "entity_type" in graph.tags.columns:
            valid_types = {"node", "resource", "commodity"}
            invalid = set(graph.tags["entity_type"].unique()) - valid_types
            if invalid:
                errors.append(ValidationError(
                    level="error",
                    category="schema",
                    entity="tags",
                    message=f"Invalid entity_type values: {invalid}"
                ))
        
        return errors
    
    # =========================================================================
    # Referential Integrity
    # =========================================================================
    
    def _validate_referential_integrity(self, graph: GraphData) -> list[ValidationError]:
        """Validate all foreign key references."""
        errors = []
        
        node_ids = set(graph.nodes["id"])
        resource_ids = set(graph.resources["id"]) if graph.resources is not None else set()
        commodity_ids = set(graph.commodities["id"]) if graph.commodities is not None else set()
        
        # Coordinates → nodes
        if graph.coordinates is not None:
            invalid = set(graph.coordinates["node_id"]) - node_ids
            if invalid:
                errors.append(ValidationError(
                    level="error",
                    category="referential",
                    entity="coordinates",
                    message=f"References unknown nodes: {list(invalid)[:5]}{'...' if len(invalid) > 5 else ''}"
                ))
        
        # Node attributes → nodes
        for name, attr in graph.node_attributes.items():
            if "node_id" in attr.data.columns:
                invalid = set(attr.data["node_id"]) - node_ids
                if invalid:
                    errors.append(ValidationError(
                        level="error",
                        category="referential",
                        entity=f"node_attributes.{name}",
                        message=f"References unknown nodes: {list(invalid)[:5]}{'...' if len(invalid) > 5 else ''}"
                    ))
        
        # Edge attributes → nodes
        for name, attr in graph.edge_attributes.items():
            if "source_id" in attr.data.columns:
                invalid = set(attr.data["source_id"]) - node_ids
                if invalid:
                    errors.append(ValidationError(
                        level="error",
                        category="referential",
                        entity=f"edge_attributes.{name}",
                        message=f"source_id references unknown nodes: {list(invalid)[:5]}"
                    ))
            
            if "target_id" in attr.data.columns:
                invalid = set(attr.data["target_id"]) - node_ids
                if invalid:
                    errors.append(ValidationError(
                        level="error",
                        category="referential",
                        entity=f"edge_attributes.{name}",
                        message=f"target_id references unknown nodes: {list(invalid)[:5]}"
                    ))
        
        # Flows → nodes
        for name, flow in graph.flows.items():
            if "source_id" in flow.data.columns:
                invalid = set(flow.data["source_id"]) - node_ids
                if invalid:
                    errors.append(ValidationError(
                        level="error",
                        category="referential",
                        entity=f"flows.{name}",
                        message=f"source_id references unknown nodes: {list(invalid)[:5]}"
                    ))
            
            if "target_id" in flow.data.columns:
                invalid = set(flow.data["target_id"]) - node_ids
                if invalid:
                    errors.append(ValidationError(
                        level="error",
                        category="referential",
                        entity=f"flows.{name}",
                        message=f"target_id references unknown nodes: {list(invalid)[:5]}"
                    ))
        
        # Demands → nodes
        if graph.demands is not None and "node_id" in graph.demands.columns:
            invalid = set(graph.demands["node_id"]) - node_ids
            if invalid:
                errors.append(ValidationError(
                    level="error",
                    category="referential",
                    entity="demands",
                    message=f"References unknown nodes: {list(invalid)[:5]}"
                ))
        
        # Inventory → nodes
        if graph.inventory is not None and "node_id" in graph.inventory.columns:
            invalid = set(graph.inventory["node_id"]) - node_ids
            if invalid:
                errors.append(ValidationError(
                    level="error",
                    category="referential",
                    entity="inventory",
                    message=f"References unknown nodes: {list(invalid)[:5]}"
                ))
        
        # Tags → entities
        if graph.tags is not None:
            node_tags = graph.tags[graph.tags["entity_type"] == "node"]
            if len(node_tags) > 0:
                invalid = set(node_tags["entity_id"]) - node_ids
                if invalid:
                    errors.append(ValidationError(
                        level="error",
                        category="referential",
                        entity="tags",
                        message=f"Node tags reference unknown nodes: {list(invalid)[:5]}"
                    ))
            
            resource_tags = graph.tags[graph.tags["entity_type"] == "resource"]
            if len(resource_tags) > 0:
                invalid = set(resource_tags["entity_id"]) - resource_ids
                if invalid:
                    errors.append(ValidationError(
                        level="error",
                        category="referential",
                        entity="tags",
                        message=f"Resource tags reference unknown resources: {list(invalid)[:5]}"
                    ))
            
            commodity_tags = graph.tags[graph.tags["entity_type"] == "commodity"]
            if len(commodity_tags) > 0:
                invalid = set(commodity_tags["entity_id"]) - commodity_ids
                if invalid:
                    errors.append(ValidationError(
                        level="error",
                        category="referential",
                        entity="tags",
                        message=f"Commodity tags reference unknown commodities: {list(invalid)[:5]}"
                    ))
        
        return errors


# =============================================================================
# Convenience function
# =============================================================================


def validate_graph(graph: GraphData, raise_on_error: bool = False) -> ValidationResult:
    """Validate a GraphData instance.
    
    Args:
        graph: GraphData to validate.
        raise_on_error: If True, raise exception on validation errors.
    
    Returns:
        ValidationResult.
    
    Example:
        >>> result = validate_graph(graph)
        >>> print(result)
        >>> 
        >>> # Or raise on error
        >>> validate_graph(graph, raise_on_error=True)
    """
    validator = GraphValidator()
    result = validator.validate(graph)
    
    if raise_on_error:
        result.raise_if_invalid()
    
    return result
