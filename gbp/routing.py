"""Distance and travel time between facilities.

One object, :class:`Routes`, answers the two queries every consumer needs:
``distance_km(source, target)`` and ``duration_periods(source, target)``.
How the answer is measured is the ``routing_mode`` of the scenario:

- ``"haversine"`` — the formula mode. Distance is the straight (great-circle)
  line between the two facilities; travel time is that distance divided by
  ``trip_speed_km_per_period``.
- ``"osrm"`` — road-network mode. Distance and riding time come from a local
  OSRM server (see ``docs/osrm_setup.md``). The full facility-to-facility
  table is fetched once, in one ``/table`` request, when the object is built;
  queries after that are matrix lookups with no network calls.

In ``osrm`` mode a pair the server cannot route (a facility that fails to
snap to a road) gets the haversine answer instead, so a query never returns
NA for a routable-looking pair. A row whose source or target id is NA (for
example ``realized_target_id`` on a ``departed`` event) is NA in both modes.

The module sits beside the model layer: it imports ``haversine_km`` from
:mod:`gbp.model` and is imported by the loaders, the simulator mechanics,
and the artifact builder.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import requests

from gbp.model import haversine_km

#: The two ways a scenario can measure distance and travel time.
ROUTING_MODES = ("haversine", "osrm")

#: The bike OSRM server started by ``scripts/osrm/serve.sh bike``.
DEFAULT_OSRM_URL = "http://127.0.0.1:5000"

_OSRM_TIMEOUT_SECONDS = 300


def _osrm_table(facilities_geo_df: pd.DataFrame, osrm_url: str) -> tuple[np.ndarray, np.ndarray]:
    """Fetch the full facility-to-facility table from an OSRM server.

    One ``/table`` request with every facility as both source and target.
    Returns two square matrices aligned to the row order of
    ``facilities_geo_df``: distance in kilometres and riding time in seconds.
    A pair the server cannot route is NaN.

    The server must allow a table this large: ``scripts/osrm/serve.sh`` starts
    it with ``--max-table-size 10000``, enough for every Citi Bike facility.
    """
    path = ";".join(
        f"{lng:.6f},{lat:.6f}" for lat, lng in facilities_geo_df[["lat", "lng"]].to_numpy()
    )
    url = f"{osrm_url}/table/v1/bike/{path}?annotations=duration,distance"
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
    """Distance and travel time for facility pairs, by the scenario's routing mode.

    Built once per scenario (``ResolvedModelData.routes``). Both queries take
    two id columns aligned on the same index and return a float Series on that
    index.

    Parameters
    ----------
    facilities_geo_df : pandas.DataFrame
        Facility geography: ``facility_id``, ``lat``, ``lng``.
    mode : {"haversine", "osrm"}, optional
        How to measure (see the module docstring). Defaults to ``"haversine"``.
    trip_speed_km_per_period : float
        Mean riding speed in km per period. Turns a haversine distance into a
        travel time; in ``osrm`` mode it only prices the fallback pairs the
        server cannot route.
    period_len : pandas.Timedelta
        Wall-clock length of one period. Turns OSRM seconds into periods.
    osrm_url : str, optional
        Base URL of the OSRM server. Only read in ``osrm`` mode.
    """

    def __init__(
        self,
        facilities_geo_df: pd.DataFrame,
        mode: str = "haversine",
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
