"""Tests for the routing modes (``gbp.routing.Routes``).

The haversine mode is pure math, tested directly. The osrm mode is tested with
a monkeypatched ``_osrm_table`` returning hand-written matrices, so no server
is needed: the tests pin down the matrix lookup, the NA-id behaviour, and the
haversine fallback for a pair the server cannot route.
"""

import numpy as np
import pandas as pd
import pytest

import gbp.routing as routing
from gbp.routing import Routes

# A ~0.2 degrees of latitude step is ~22.24 km along the meridian.
GEO = pd.DataFrame(
    {
        "facility_id": ["A", "B", "C"],
        "lat": [40.0, 40.2, 40.4],
        "lng": [-74.0, -74.0, -74.0],
    }
)
HOUR = pd.Timedelta(hours=1)


def _pairs(sources: list, targets: list) -> tuple[pd.Series, pd.Series]:
    return pd.Series(sources, dtype="object"), pd.Series(targets, dtype="object")


def test_unknown_mode_is_rejected():
    with pytest.raises(ValueError, match="routing mode"):
        Routes(GEO, "teleport", trip_speed_km_per_period=10.0, period_len=HOUR)


def test_haversine_mode_distance_and_duration():
    routes = Routes(GEO, "haversine", trip_speed_km_per_period=10.0, period_len=HOUR)
    source, target = _pairs(["A", "A"], ["B", "C"])
    distance = routes.distance_km(source, target)
    assert distance.round(1).tolist() == [22.2, 44.5]
    # Duration is distance over speed, not rounded.
    duration = routes.duration_periods(source, target)
    assert np.allclose(duration, distance / 10.0)


def test_na_id_gives_na_answer():
    routes = Routes(GEO, "haversine", trip_speed_km_per_period=10.0, period_len=HOUR)
    source, target = _pairs(["A"], [pd.NA])
    assert routes.distance_km(source, target).isna().all()
    assert routes.duration_periods(source, target).isna().all()


@pytest.fixture
def osrm_routes(monkeypatch):
    """Osrm-mode routes over a fake 3x3 table; the A->C pair is unroutable."""
    distance_km = np.array(
        [
            [0.0, 30.0, np.nan],
            [30.0, 0.0, 50.0],
            [np.nan, 50.0, 0.0],
        ]
    )
    duration_sec = np.array(
        [
            [0.0, 7200.0, np.nan],
            [7200.0, 0.0, 10800.0],
            [np.nan, 10800.0, 0.0],
        ]
    )
    monkeypatch.setattr(routing, "_osrm_table", lambda geo, url: (distance_km, duration_sec))
    return Routes(GEO, "osrm", trip_speed_km_per_period=10.0, period_len=HOUR)


def test_osrm_mode_reads_the_table(osrm_routes):
    source, target = _pairs(["A", "B"], ["B", "C"])
    assert osrm_routes.distance_km(source, target).tolist() == [30.0, 50.0]
    # 7200 s and 10800 s over one-hour periods.
    assert osrm_routes.duration_periods(source, target).tolist() == [2.0, 3.0]


def test_osrm_unroutable_pair_falls_back_to_haversine(osrm_routes):
    source, target = _pairs(["A"], ["C"])
    distance = osrm_routes.distance_km(source, target)
    assert distance.round(1).tolist() == [44.5]
    duration = osrm_routes.duration_periods(source, target)
    assert np.allclose(duration, distance / 10.0)


def test_osrm_na_id_stays_na(osrm_routes):
    source, target = _pairs(["A"], [pd.NA])
    assert osrm_routes.distance_km(source, target).isna().all()
    assert osrm_routes.duration_periods(source, target).isna().all()
