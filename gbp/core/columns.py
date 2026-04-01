"""Canonical column-name constants used across the codebase.

Import from here to avoid typos in string literals::

    from gbp.core.columns import FACILITY_ID, COMMODITY_CATEGORY
    df.groupby([FACILITY_ID, COMMODITY_CATEGORY])

Only the most widely used names are listed.  Domain-specific or
single-use column names should remain as local string literals.
"""

# ── Entity identifiers ───────────────────────────────────────────────
FACILITY_ID = "facility_id"
FACILITY_TYPE = "facility_type"
COMMODITY_CATEGORY = "commodity_category"
COMMODITY_CATEGORY_ID = "commodity_category_id"
RESOURCE_CATEGORY = "resource_category"
RESOURCE_CATEGORY_ID = "resource_category_id"
RESOURCE_ID = "resource_id"

# ── Edge identifiers ─────────────────────────────────────────────────
SOURCE_ID = "source_id"
TARGET_ID = "target_id"
MODAL_TYPE = "modal_type"

# ── Temporal ─────────────────────────────────────────────────────────
PERIOD_ID = "period_id"
PERIOD_INDEX = "period_index"
DATE = "date"

# ── Measures ─────────────────────────────────────────────────────────
QUANTITY = "quantity"

# ── Behavior ─────────────────────────────────────────────────────────
OPERATION_TYPE = "operation_type"
