"""Business validation for raw model data before resolution."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from gbp.core.enums import FacilityRole
from gbp.core.model import RawModelData


@dataclass
class ValidationError:
    """Single validation issue."""

    level: str  # "error" or "warning"
    category: str
    entity: str
    message: str


@dataclass
class ValidationResult:
    """Aggregated validation outcome."""

    errors: list[ValidationError] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """True if there are no blocking errors."""
        return not any(e.level == "error" for e in self.errors)

    @property
    def has_warnings(self) -> bool:
        """True if any warning-level issues exist."""
        return any(e.level == "warning" for e in self.errors)

    def raise_if_invalid(self) -> None:
        """Raise ValueError if any error-level issues exist."""
        blocking = [e for e in self.errors if e.level == "error"]
        if blocking:
            msg = "; ".join(f"{e.category}: {e.message}" for e in blocking)
            raise ValueError(f"Raw model validation failed: {msg}")


def _facility_roles_map(facility_roles: pd.DataFrame) -> dict[str, set[str]]:
    """Map facility_id -> set of role value strings."""
    return (
        facility_roles
        .assign(
            facility_id=facility_roles["facility_id"].astype(str),
            role=facility_roles["role"].astype(str),
        )
        .groupby("facility_id")["role"]
        .apply(set)
        .to_dict()
    )


def _check_referential_integrity(raw: RawModelData, result: ValidationResult) -> None:
    """Roles and foreign keys for facilities and edges."""
    facilities = set(raw.facilities["facility_id"].astype(str))
    roles_map = _facility_roles_map(raw.facility_roles)

    def _fk_role_check(
        df: pd.DataFrame | None,
        entity: str,
        required_role: str,
        role_msg: str,
    ) -> None:
        if df is None or df.empty:
            return
        fids = df["facility_id"].astype(str).unique()
        unknown = set(fids) - facilities
        for fid in sorted(unknown):
            result.errors.append(
                ValidationError("error", "referential", entity, f"Unknown facility_id {fid}")
            )
        for fid in sorted(set(fids) - unknown):
            if required_role not in roles_map.get(fid, set()):
                result.errors.append(
                    ValidationError("error", "referential", entity, role_msg.format(fid=fid))
                )

    _fk_role_check(
        raw.demand, "demand", FacilityRole.SINK.value,
        "Facility {fid} has demand but not SINK role",
    )
    _fk_role_check(
        raw.supply, "supply", FacilityRole.SOURCE.value,
        "Facility {fid} has supply but not SOURCE role",
    )
    _fk_role_check(
        raw.inventory_initial, "inventory_initial", FacilityRole.STORAGE.value,
        "Facility {fid} has initial inventory but not STORAGE role",
    )

    if raw.edges is not None and not raw.edges.empty:
        bad_src = set(raw.edges["source_id"].astype(str)) - facilities
        for s in sorted(bad_src):
            result.errors.append(
                ValidationError("error", "referential", "edges", f"Unknown source_id {s}")
            )
        bad_tgt = set(raw.edges["target_id"].astype(str)) - facilities
        for t in sorted(bad_tgt):
            result.errors.append(
                ValidationError("error", "referential", "edges", f"Unknown target_id {t}")
            )


def _check_resource_completeness(raw: RawModelData, result: ValidationResult) -> None:
    """Warn if edge x commodity has no compatible resource."""
    if (
        raw.edges is None
        or raw.edge_commodities is None
        or raw.resource_commodity_compatibility is None
        or raw.resource_modal_compatibility is None
    ):
        return

    rcc = raw.resource_commodity_compatibility
    rmc = raw.resource_modal_compatibility

    # Build set of (commodity_category, modal_type) pairs that have a compatible resource
    # by joining resource_category across the two compatibility tables.
    rc_for_cc = rcc[["resource_category", "commodity_category"]].astype(str)
    rc_for_modal = rmc[["resource_category", "modal_type"]].astype(str)
    covered = (
        rc_for_cc.merge(rc_for_modal, on="resource_category")
        [["commodity_category", "modal_type"]]
        .drop_duplicates()
    )
    covered_set = set(zip(covered["commodity_category"], covered["modal_type"], strict=False))

    # Filter to enabled edge-commodities that exist on actual edges
    ec = raw.edge_commodities.copy()
    if "enabled" in ec.columns:
        ec = ec[ec["enabled"].fillna(True).astype(bool)]
    ec = ec.assign(
        source_id=ec["source_id"].astype(str),
        target_id=ec["target_id"].astype(str),
        modal_type=ec["modal_type"].astype(str),
        commodity_category=ec["commodity_category"].astype(str),
    )
    # Semi-join with edges to keep only rows matching real edges
    edge_keys = raw.edges[["source_id", "target_id", "modal_type"]].astype(str)
    ec = ec.merge(edge_keys, on=["source_id", "target_id", "modal_type"])

    # Check coverage
    ec["_covered"] = [
        (cc, mt) in covered_set
        for cc, mt in zip(ec["commodity_category"], ec["modal_type"], strict=False)
    ]
    uncovered = ec[~ec["_covered"]]
    for _, row in uncovered.iterrows():
        result.errors.append(
            ValidationError(
                level="warning",
                category="resource",
                entity="edge_commodities",
                message=(
                    f"No resource_category carries {row['commodity_category']} "
                    f"on modal {row['modal_type']} "
                    f"for edge {row['source_id']}->{row['target_id']}"
                ),
            )
        )


def _check_temporal_coverage(raw: RawModelData, result: ValidationResult) -> None:
    """Warn if demand/supply dates do not span planning horizon."""
    if raw.planning_horizon is None or raw.planning_horizon.empty:
        return
    start = pd.to_datetime(raw.planning_horizon.iloc[0]["start_date"]).normalize()
    end = pd.to_datetime(raw.planning_horizon.iloc[0]["end_date"]).normalize()

    for name, df in (("demand", raw.demand), ("supply", raw.supply)):
        if df is None or df.empty or "date" not in df.columns:
            continue
        dates = pd.to_datetime(df["date"])
        if dates.min() > start or dates.max() < end - pd.Timedelta(days=1):
            result.errors.append(
                ValidationError(
                    level="warning",
                    category="temporal",
                    entity=name,
                    message=(
                        "Dates may not cover full planning horizon "
                        f"[{start.date()}, {end.date()})"
                    ),
                )
            )


def _check_graph_connectivity(raw: RawModelData, result: ValidationResult) -> None:
    """Warn if a SINK is unreachable from any SOURCE via enabled edge commodities."""
    roles_map = _facility_roles_map(raw.facility_roles)
    sinks = {fid for fid, rs in roles_map.items() if FacilityRole.SINK.value in rs}
    sources = {fid for fid, rs in roles_map.items() if FacilityRole.SOURCE.value in rs}
    if not sinks or not sources:
        return
    if raw.edge_commodities is None or raw.edge_commodities.empty:
        return

    ec = raw.edge_commodities
    if "enabled" in ec.columns:
        ec = ec[ec["enabled"].fillna(True).astype(bool)]
    edges_s = ec["source_id"].astype(str)
    edges_t = ec["target_id"].astype(str)
    adj: dict[str, set[str]] = {n: set() for n in sinks | sources}
    for s, t in zip(edges_s, edges_t, strict=False):
        adj.setdefault(s, set()).add(t)
        adj.setdefault(t, set()).add(s)

    from collections import deque

    reachable: set[str] = set()
    q = deque(sources)
    while q:
        u = q.popleft()
        if u in reachable:
            continue
        reachable.add(u)
        for v in adj.get(u, ()):
            if v not in reachable:
                q.append(v)

    for sk in sinks:
        if sk not in reachable:
            result.errors.append(
                ValidationError(
                    level="warning",
                    category="connectivity",
                    entity="graph",
                    message=f"SINK facility {sk} may be unreachable from any SOURCE",
                )
            )


def _check_transformation_consistency(raw: RawModelData, result: ValidationResult) -> None:
    """Transformation inputs/outputs appear on edge commodities for the facility."""
    if (
        raw.transformations is None
        or raw.transformation_inputs is None
        or raw.transformation_outputs is None
        or raw.edge_commodities is None
    ):
        return

    ec = raw.edge_commodities
    if ec.empty:
        return

    # Sets of (facility_id, commodity_category) reachable via incoming/outgoing edges
    incoming_pairs = set(zip(
        ec["target_id"].astype(str), ec["commodity_category"].astype(str), strict=False,
    ))
    outgoing_pairs = set(zip(
        ec["source_id"].astype(str), ec["commodity_category"].astype(str), strict=False,
    ))

    # Join transformations with inputs/outputs to get (facility_id, tid, commodity_category)
    t_fid = raw.transformations[["transformation_id", "facility_id"]].astype(str)

    ti = raw.transformation_inputs[["transformation_id", "commodity_category"]].astype(str)
    ti_with_fid = ti.merge(t_fid, on="transformation_id")
    for _, row in ti_with_fid.iterrows():
        if (row["facility_id"], row["commodity_category"]) not in incoming_pairs:
            result.errors.append(
                ValidationError(
                    level="error",
                    category="transformation",
                    entity="transformation_inputs",
                    message=(
                        f"Transformation {row['transformation_id']} at "
                        f"{row['facility_id']}: input commodity "
                        f"{row['commodity_category']} not on any incoming edge_commodity"
                    ),
                )
            )

    to = raw.transformation_outputs[["transformation_id", "commodity_category"]].astype(str)
    to_with_fid = to.merge(t_fid, on="transformation_id")
    for _, row in to_with_fid.iterrows():
        if (row["facility_id"], row["commodity_category"]) not in outgoing_pairs:
            result.errors.append(
                ValidationError(
                    level="error",
                    category="transformation",
                    entity="transformation_outputs",
                    message=(
                        f"Transformation {row['transformation_id']} at "
                        f"{row['facility_id']}: output commodity "
                        f"{row['commodity_category']} not on any outgoing edge_commodity"
                    ),
                )
            )


def _check_observations(raw: RawModelData, result: ValidationResult) -> None:
    """FK checks for observed_flow and observed_inventory (warnings only)."""
    facilities = set(raw.facilities["facility_id"].astype(str))
    cat_ids = set(raw.commodity_categories["commodity_category_id"].astype(str))

    if raw.observed_flow is not None and not raw.observed_flow.empty:
        bad_src = set(raw.observed_flow["source_id"].astype(str)) - facilities
        for sid in sorted(bad_src):
            result.errors.append(
                ValidationError("warning", "referential", "observed_flow",
                                f"Unknown source_id {sid}"),
            )
        bad_tgt = set(raw.observed_flow["target_id"].astype(str)) - facilities
        for tid in sorted(bad_tgt):
            result.errors.append(
                ValidationError("warning", "referential", "observed_flow",
                                f"Unknown target_id {tid}"),
            )
        bad_cc = set(raw.observed_flow["commodity_category"].astype(str)) - cat_ids
        for cc in sorted(bad_cc):
            result.errors.append(
                ValidationError("warning", "referential", "observed_flow",
                                f"Unknown commodity_category {cc}"),
            )
        if (
            raw.resources is not None
            and not raw.resources.empty
            and "resource_id" in raw.observed_flow.columns
        ):
            resource_ids = set(raw.resources["resource_id"].astype(str))
            flow_rids = raw.observed_flow["resource_id"].dropna().astype(str)
            bad_rid = set(flow_rids) - resource_ids
            for rid in sorted(bad_rid):
                result.errors.append(
                    ValidationError("warning", "referential", "observed_flow",
                                    f"Unknown resource_id {rid}"),
                )

    if raw.observed_inventory is not None and not raw.observed_inventory.empty:
        bad_fid = (
            set(raw.observed_inventory["facility_id"].astype(str)) - facilities
        )
        for fid in sorted(bad_fid):
            result.errors.append(
                ValidationError("warning", "referential", "observed_inventory",
                                f"Unknown facility_id {fid}"),
            )
        bad_cc = (
            set(raw.observed_inventory["commodity_category"].astype(str)) - cat_ids
        )
        for cc in sorted(bad_cc):
            result.errors.append(
                ValidationError("warning", "referential", "observed_inventory",
                                f"Unknown commodity_category {cc}"),
            )


def validate_raw_model(raw: RawModelData) -> ValidationResult:
    """Run all validation checks on ``raw`` (errors block build; warnings do not)."""
    result = ValidationResult()
    _check_referential_integrity(raw, result)
    _check_resource_completeness(raw, result)
    _check_temporal_coverage(raw, result)
    _check_graph_connectivity(raw, result)
    _check_transformation_consistency(raw, result)
    _check_observations(raw, result)
    return result
