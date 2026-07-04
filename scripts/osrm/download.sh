#!/usr/bin/env bash
# Download the New York City metro map extract from BBBike.
# The extract covers the whole Citi Bike service area, including
# Jersey City and Hoboken on the New Jersey side of the Hudson.
# Coverage box: longitude -74.36 .. -73.67, latitude 40.48 .. 40.96.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OSRM_DIR="$REPO_ROOT/data/osrm"

mkdir -p "$OSRM_DIR"
curl -L -o "$OSRM_DIR/NewYork.osm.pbf" \
    https://download.bbbike.org/osm/bbbike/NewYork/NewYork.osm.pbf
ls -lh "$OSRM_DIR/NewYork.osm.pbf"
