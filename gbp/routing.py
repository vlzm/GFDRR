"""Distance and travel time between facilities, by the scenario's haversine or OSRM routing mode."""

from __future__ import annotations

import os
from typing import Literal

import numpy as np
import pandas as pd
import requests

from gbp.model import haversine_km

#: The two ways a scenario can measure distance and travel time.
RoutingMode = Literal["haversine", "osrm"]
ROUTING_MODES = ("haversine", "osrm")

#: Base URL of the OSRM server. Set the ``OSRM_URL`` environment variable to
#: point somewhere else (in a container the server is not on localhost).
DEFAULT_OSRM_URL = os.environ.get("OSRM_URL", "http://127.0.0.1:5000")

#: Travel profile the OSRM server was built with; it is part of the table URL.
#: Set the ``OSRM_PROFILE`` environment variable to match the running server.
#: The default matches ``scripts/osrm/serve.sh bike``, the server this repo
#: ships -- a deployment setting, not something the model knows about.
DEFAULT_OSRM_PROFILE = os.environ.get("OSRM_PROFILE", "bike")

_OSRM_TIMEOUT_SECONDS = 300


def _osrm_table(facilities_geo_df: pd.DataFrame, osrm_url: str) -> tuple[np.ndarray, np.ndarray]:
    """Fetch the full facility-to-facility table (distance km, duration seconds) from OSRM."""
    path = ";".join(
        f"{lng:.6f},{lat:.6f}" for lat, lng in facilities_geo_df[["lat", "lng"]].to_numpy()
    )
    url = f"{osrm_url}/table/v1/{DEFAULT_OSRM_PROFILE}/{path}?annotations=duration,distance"
    response = requests.get(url, timeout=_OSRM_TIMEOUT_SECONDS)
    if response.status_code != 200:
        raise ValueError(
            f"OSRM table request failed ({response.status_code}): {response.text[:200]}"
        )
    body = response.json()
    if body.get("code") != "Ok":
        raise ValueError(f"OSRM table request returned code {body.get('code')!r}")
    # json null (unroutable pair) becomes NaN in a float array.
    duration_sec = np.array(body["durations"], dtype="float64")
    distance_km = np.array(body["distances"], dtype="float64") / 1000.0
    return distance_km, duration_sec


class Routes:
    """Distance and travel time for facility pairs, by the scenario's routing mode."""

    def __init__(
        self,
        facilities_geo_df: pd.DataFrame,
        mode: RoutingMode = "haversine",
        *,
        trip_speed_km_per_period: float,
        period_len: pd.Timedelta,
        osrm_url: str = DEFAULT_OSRM_URL,
    ) -> None:
        if mode not in ROUTING_MODES:
            raise ValueError(f"routing mode must be one of {ROUTING_MODES}, got {mode!r}")
        self.mode = mode
        self.trip_speed_km_per_period = float(trip_speed_km_per_period)
        self._coords = facilities_geo_df.set_index("facility_id")[["lat", "lng"]]
        if mode == "osrm":
            distance_km, duration_sec = _osrm_table(facilities_geo_df, osrm_url)
            self._matrix_position = {
                facility_id: position
                for position, facility_id in enumerate(facilities_geo_df["facility_id"])
            }
            self._distance_km_matrix = distance_km
            self._duration_periods_matrix = duration_sec / period_len.total_seconds()

    def distance_km(self, source: pd.Series, target: pd.Series) -> pd.Series:
        """Distance in kilometres for each (source, target) pair, aligned to ``source``."""
        straight = self._haversine_km(source, target)
        if self.mode == "haversine":
            return straight
        return self._lookup(self._distance_km_matrix, source, target).fillna(straight)

    def duration_periods(self, source: pd.Series, target: pd.Series) -> pd.Series:
        """Travel time in periods (float, not rounded), aligned to ``source``."""
        estimate = self._haversine_km(source, target) / self.trip_speed_km_per_period
        if self.mode == "haversine":
            return estimate
        return self._lookup(self._duration_periods_matrix, source, target).fillna(estimate)

    def _haversine_km(self, source: pd.Series, target: pd.Series) -> pd.Series:
        """Straight-line distance between the two facilities of each row."""
        return haversine_km(
            source.map(self._coords["lat"]),
            source.map(self._coords["lng"]),
            target.map(self._coords["lat"]),
            target.map(self._coords["lng"]),
        )

    def _lookup(self, matrix: np.ndarray, source: pd.Series, target: pd.Series) -> pd.Series:
        """Read the OSRM matrix at each row's (source, target) cell; NA ids give NaN."""
        source_position = source.map(self._matrix_position)
        target_position = target.map(self._matrix_position)
        known = source_position.notna() & target_position.notna()
        out = pd.Series(np.nan, index=source.index, dtype="float64")
        out[known] = matrix[
            source_position[known].astype("int64").to_numpy(),
            target_position[known].astype("int64").to_numpy(),
        ]
        return out
