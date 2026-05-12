"""Shared distance utilities for the simulator.

Centralises Haversine computation, edge-distance lookup construction,
and full N×N distance-matrix building so that :class:`OverflowRedirectPhase`
and :class:`RebalancerTask` share a single implementation.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd


def haversine_distance_m(
    a: tuple[float, float],
    b: tuple[float, float],
) -> float:
    """Compute great-circle distance in metres between two points.

    Parameters
    ----------
    a
        First point as ``(latitude, longitude)`` in degrees.
    b
        Second point as ``(latitude, longitude)`` in degrees.

    Returns
    -------
    float
        Distance in metres.
    """
    lat1 = math.radians(a[0])
    lon1 = math.radians(a[1])
    lat2 = math.radians(b[0])
    lon2 = math.radians(b[1])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    s = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    return 6_371_000.0 * 2.0 * math.atan2(math.sqrt(s), math.sqrt(1.0 - s))


def build_edge_distance_map(
    distance_matrix: pd.DataFrame | None,
) -> dict[tuple[str, str], float]:
    """Convert a distance-matrix DataFrame to an edge-keyed dict.

    Parameters
    ----------
    distance_matrix
        Optional DataFrame with ``source_id``, ``target_id``, and
        ``distance`` columns (distances in km).

    Returns
    -------
    dict of (str, str) to float
        Mapping ``{(source_id, target_id): distance_km}``.
        Empty dict when the input is ``None`` or empty.
    """
    if distance_matrix is None or distance_matrix.empty:
        return {}
    return {
        (str(row["source_id"]), str(row["target_id"])): float(row["distance"])
        for _, row in distance_matrix.iterrows()
    }


def build_node_distance_matrix(
    locations: list[tuple[float, float]],
    node_ids: list[str | None],
    edge_distances: dict[tuple[str, str], float],
) -> np.ndarray:
    """Build an NxN integer distance matrix in metres for arbitrary nodes.

    Uses ``edge_distances`` (km) when both nodes have an id and a matching
    edge exists; falls back to Haversine otherwise.

    Parameters
    ----------
    locations
        List of ``(latitude, longitude)`` tuples for each node.
    node_ids
        Parallel list of graph node ids (may contain ``None`` entries).
    edge_distances
        Pre-computed edge distances in km, keyed by ``(source, target)``.

    Returns
    -------
    np.ndarray
        Integer distance matrix of shape ``(N, N)`` in metres.
    """
    n = len(locations)
    matrix = np.zeros((n, n), dtype=int)
    for i in range(n):
        for j in range(n):
            if i == j:
                continue
            sni = node_ids[i]
            snj = node_ids[j]
            km: float | None = None
            if sni is not None and snj is not None:
                km = edge_distances.get((sni, snj))
            if km is not None:
                matrix[i, j] = int(round(km * 1000.0))
            else:
                matrix[i, j] = int(round(
                    haversine_distance_m(locations[i], locations[j])
                ))
    return matrix


def build_facility_distance_matrix(
    facilities: pd.DataFrame,
    distance_matrix: pd.DataFrame | None,
) -> tuple[list[str], np.ndarray]:
    """Return sorted facility ids and a square distance matrix in km.

    Uses ``distance_matrix`` when available; falls back to Haversine
    from facility ``lat`` / ``lon`` coordinates.

    Parameters
    ----------
    facilities
        DataFrame with at least ``facility_id`` column.  If ``lat`` and
        ``lon`` columns are present they are used as Haversine fallback.
    distance_matrix
        Optional DataFrame with ``source_id``, ``target_id``, ``distance``
        columns (distances in km).

    Returns
    -------
    tuple of (list[str], np.ndarray)
        ``(sorted_facility_ids, N×N float distance array)``.
        Diagonal is ``0.0``; missing pairs are ``np.inf``.
    """
    facility_ids = sorted(facilities["facility_id"].astype(str).unique().tolist())
    n = len(facility_ids)

    edge_distances = build_edge_distance_map(distance_matrix)

    lat_lookup: dict[str, float] = {}
    lon_lookup: dict[str, float] = {}
    if (
        facilities is not None
        and not facilities.empty
        and "lat" in facilities.columns
        and "lon" in facilities.columns
    ):
        facs = facilities.dropna(subset=["lat", "lon"])
        for _, fac_row in facs.iterrows():
            fid = str(fac_row["facility_id"])
            lat_lookup[fid] = float(fac_row["lat"])
            lon_lookup[fid] = float(fac_row["lon"])

    matrix = np.full((n, n), np.inf, dtype=float)
    for i, fi in enumerate(facility_ids):
        matrix[i, i] = 0.0
        for j, fj in enumerate(facility_ids):
            if i == j:
                continue
            d_km = edge_distances.get((fi, fj))
            if d_km is None and fi in lat_lookup and fj in lat_lookup:
                d_km = (
                    haversine_distance_m(
                        (lat_lookup[fi], lon_lookup[fi]),
                        (lat_lookup[fj], lon_lookup[fj]),
                    )
                    / 1000.0
                )
            if d_km is not None:
                matrix[i, j] = float(d_km)

    return facility_ids, matrix
