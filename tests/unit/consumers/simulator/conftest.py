"""Shared fixtures for simulator tests."""

from __future__ import annotations

import dataclasses

import pandas as pd
import pytest

from gbp.build.pipeline import build_model
from gbp.core.model import ResolvedModelData
from tests.unit.build.fixtures import minimal_raw_model


@pytest.fixture()
def resolved_model() -> ResolvedModelData:
    """Build a resolved model from the minimal bike-sharing fixture.

    Extends the base fixture with ``inventory_initial`` so that the simulator
    has starting stock to work with.
    """
    raw = minimal_raw_model(with_demand=True, with_supply=False)

    # Inject inventory_initial (not present in the base fixture)
    raw = dataclasses.replace(
        raw,
        inventory_initial=pd.DataFrame(
            {
                "facility_id": ["d1", "s1", "s2"],
                "commodity_category": ["working_bike"] * 3,
                "quantity": [50.0, 12.0, 7.0],
            }
        ),
    )

    return build_model(raw)
