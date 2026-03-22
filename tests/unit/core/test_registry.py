"""Tests for ``AttributeRegistry``."""

from __future__ import annotations

import pandas as pd
import pytest

from gbp.core.attributes.registry import AttributeRegistry, RegisteredAttribute
from gbp.core.enums import AttributeKind


def _cost_df() -> pd.DataFrame:
    return pd.DataFrame({
        "facility_id": ["s1", "s1", "s2"],
        "operation_type": ["visit", "handling", "visit"],
        "commodity_category": ["bike", "bike", "bike"],
        "date": ["2025-01-01", "2025-01-01", "2025-01-01"],
        "cost_per_unit": [1.0, 2.0, 1.5],
    })


def _capacity_df() -> pd.DataFrame:
    return pd.DataFrame({
        "facility_id": ["s1", "s2"],
        "operation_type": ["storage", "storage"],
        "commodity_category": ["bike", "bike"],
        "capacity": [20.0, 30.0],
    })


class TestRegister:
    def test_register_basic(self) -> None:
        reg = AttributeRegistry()
        reg.register(
            name="op_cost",
            data=_cost_df(),
            entity_type="facility",
            kind=AttributeKind.COST,
            grain=("facility_id", "operation_type", "commodity_category", "date"),
            value_column="cost_per_unit",
        )
        assert "op_cost" in reg
        assert len(reg) == 1

    def test_resolved_grain_computed(self) -> None:
        """``date`` in grain is mapped to ``period_id`` in resolved_grain."""
        reg = AttributeRegistry()
        reg.register(
            name="op_cost",
            data=_cost_df(),
            entity_type="facility",
            kind=AttributeKind.COST,
            grain=("facility_id", "operation_type", "commodity_category", "date"),
            value_column="cost_per_unit",
        )
        spec = reg.get("op_cost").spec
        assert spec.resolved_grain == (
            "facility_id", "operation_type", "commodity_category", "period_id",
        )
        assert spec.time_varying is True

    def test_non_time_varying(self) -> None:
        reg = AttributeRegistry()
        reg.register(
            name="op_cap",
            data=_capacity_df(),
            entity_type="facility",
            kind=AttributeKind.CAPACITY,
            grain=("facility_id", "operation_type", "commodity_category"),
            value_column="capacity",
        )
        spec = reg.get("op_cap").spec
        assert spec.time_varying is False
        assert spec.resolved_grain == spec.grain

    def test_missing_columns_raises(self) -> None:
        reg = AttributeRegistry()
        with pytest.raises(ValueError, match="missing columns"):
            reg.register(
                name="bad",
                data=pd.DataFrame({"x": [1]}),
                entity_type="facility",
                kind=AttributeKind.COST,
                grain=("facility_id",),
                value_column="cost",
            )

    def test_negative_cost_raises(self) -> None:
        df = _cost_df().copy()
        df.loc[0, "cost_per_unit"] = -5.0
        reg = AttributeRegistry()
        with pytest.raises(ValueError, match="negative"):
            reg.register(
                name="bad_cost",
                data=df,
                entity_type="facility",
                kind=AttributeKind.COST,
                grain=("facility_id", "operation_type", "commodity_category", "date"),
                value_column="cost_per_unit",
            )

    def test_zero_capacity_raises(self) -> None:
        df = _capacity_df().copy()
        df.loc[0, "capacity"] = 0.0
        reg = AttributeRegistry()
        with pytest.raises(ValueError, match="non-positive"):
            reg.register(
                name="bad_cap",
                data=df,
                entity_type="facility",
                kind=AttributeKind.CAPACITY,
                grain=("facility_id", "operation_type", "commodity_category"),
                value_column="capacity",
            )

    def test_source_table_equals_name(self) -> None:
        reg = AttributeRegistry()
        reg.register(
            name="my_attr",
            data=_capacity_df(),
            entity_type="facility",
            kind=AttributeKind.ADDITIONAL,
            grain=("facility_id", "operation_type", "commodity_category"),
            value_column="capacity",
        )
        assert reg.get("my_attr").spec.source_table == "my_attr"


class TestLookups:
    @pytest.fixture()
    def registry(self) -> AttributeRegistry:
        reg = AttributeRegistry()
        reg.register(
            name="op_cost",
            data=_cost_df(),
            entity_type="facility",
            kind=AttributeKind.COST,
            grain=("facility_id", "operation_type", "commodity_category", "date"),
            value_column="cost_per_unit",
        )
        reg.register(
            name="op_cap",
            data=_capacity_df(),
            entity_type="facility",
            kind=AttributeKind.CAPACITY,
            grain=("facility_id", "operation_type", "commodity_category"),
            value_column="capacity",
        )
        return reg

    def test_get(self, registry: AttributeRegistry) -> None:
        attr = registry.get("op_cost")
        assert isinstance(attr, RegisteredAttribute)
        assert attr.spec.name == "op_cost"
        assert len(attr.data) == 3

    def test_get_missing_raises(self, registry: AttributeRegistry) -> None:
        with pytest.raises(KeyError):
            registry.get("nonexistent")

    def test_get_by_entity(self, registry: AttributeRegistry) -> None:
        fac = registry.get_by_entity("facility")
        assert len(fac) == 2
        assert all(a.spec.entity_type == "facility" for a in fac)
        assert registry.get_by_entity("edge") == []

    def test_get_by_kind(self, registry: AttributeRegistry) -> None:
        costs = registry.get_by_kind(AttributeKind.COST)
        assert len(costs) == 1
        assert costs[0].spec.name == "op_cost"

    def test_specs_property(self, registry: AttributeRegistry) -> None:
        specs = registry.specs
        assert len(specs) == 2
        assert {s.name for s in specs} == {"op_cost", "op_cap"}

    def test_names_property(self, registry: AttributeRegistry) -> None:
        assert set(registry.names) == {"op_cost", "op_cap"}

    def test_contains(self, registry: AttributeRegistry) -> None:
        assert "op_cost" in registry
        assert "missing" not in registry

    def test_bool(self) -> None:
        assert not AttributeRegistry()
        reg = AttributeRegistry()
        reg.register(
            name="x",
            data=_capacity_df(),
            entity_type="facility",
            kind=AttributeKind.ADDITIONAL,
            grain=("facility_id", "operation_type", "commodity_category"),
            value_column="capacity",
        )
        assert reg

    def test_to_dict(self, registry: AttributeRegistry) -> None:
        d = registry.to_dict()
        assert set(d.keys()) == {"op_cost", "op_cap"}
        assert isinstance(d["op_cost"], pd.DataFrame)

    def test_summary(self, registry: AttributeRegistry) -> None:
        s = registry.summary()
        assert "op_cost" in s
        assert "op_cap" in s
        assert "COST" in s
        assert "CAPACITY" in s

    def test_copy(self, registry: AttributeRegistry) -> None:
        copied = registry.copy()
        assert copied.names == registry.names
        assert copied is not registry


class TestEdgeAttribute:
    def test_edge_grain(self) -> None:
        reg = AttributeRegistry()
        df = pd.DataFrame({
            "source_id": ["a"],
            "target_id": ["b"],
            "modal_type": ["road"],
            "date": ["2025-01-01"],
            "cost_per_unit": [5.0],
        })
        reg.register(
            name="transport_cost",
            data=df,
            entity_type="edge",
            kind=AttributeKind.COST,
            grain=("source_id", "target_id", "modal_type", "date"),
            value_column="cost_per_unit",
        )
        spec = reg.get("transport_cost").spec
        assert spec.entity_grain == ("source_id", "target_id", "modal_type")


class TestResourceAttribute:
    def test_resource_grain(self) -> None:
        reg = AttributeRegistry()
        df = pd.DataFrame({
            "resource_category": ["truck"],
            "value": [100.0],
        })
        reg.register(
            name="resource_cap",
            data=df,
            entity_type="resource",
            kind=AttributeKind.CAPACITY,
            grain=("resource_category",),
            value_column="value",
        )
        spec = reg.get("resource_cap").spec
        assert spec.entity_grain == ("resource_category",)


class TestIntegrationWithModel:
    def test_raw_model_with_registry(self) -> None:
        """AttributeRegistry can be attached to RawModelData."""
        from tests.unit.build.fixtures import minimal_raw_model

        raw = minimal_raw_model()
        raw.attributes.register(
            name="custom_cost",
            data=pd.DataFrame({
                "facility_id": ["d1"],
                "date": ["2025-01-01"],
                "cost_per_unit": [10.0],
            }),
            entity_type="facility",
            kind=AttributeKind.COST,
            grain=("facility_id", "date"),
            value_column="cost_per_unit",
        )
        assert "custom_cost" in raw.attributes
        assert "custom_cost" in raw.populated_tables

    def test_build_with_registry_attributes(self) -> None:
        """build_model resolves registry attributes."""
        from gbp.build.pipeline import build_model
        from tests.unit.build.fixtures import minimal_raw_model

        raw = minimal_raw_model()
        raw.attributes.register(
            name="custom_cost",
            data=pd.DataFrame({
                "facility_id": ["d1"],
                "date": ["2025-01-01"],
                "cost_per_unit": [10.0],
            }),
            entity_type="facility",
            kind=AttributeKind.COST,
            grain=("facility_id", "date"),
            value_column="cost_per_unit",
        )
        resolved = build_model(raw)
        assert "custom_cost" in resolved.attributes
        custom = resolved.attributes.get("custom_cost")
        assert "period_id" in custom.data.columns

    def test_table_summary_shows_registry(self) -> None:
        from tests.unit.build.fixtures import minimal_raw_model

        raw = minimal_raw_model()
        raw.attributes.register(
            name="demo_attr",
            data=pd.DataFrame({
                "facility_id": ["s1"],
                "capacity": [10.0],
            }),
            entity_type="facility",
            kind=AttributeKind.CAPACITY,
            grain=("facility_id",),
            value_column="capacity",
        )
        summary = raw.table_summary()
        assert "AttributeRegistry" in summary
        assert "demo_attr" in summary
