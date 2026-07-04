#!/usr/bin/env bash
# Build the OSRM routing graph for one travel profile.
#
# Usage: ./build.sh bike|truck
#
# OSRM bakes the travel profile (which roads are allowed, at what speed)
# into the graph at build time. One graph serves one profile, so bikes
# and trucks each get their own graph in their own directory:
#   data/osrm/bike/NewYork.osrm.*
#   data/osrm/truck/NewYork.osrm.*
#
# Two build steps, same as in the OSRM docs:
#   osrm-extract  — read the raw map, keep the roads this profile can use
#   osrm-contract — precompute shortcuts so route queries are fast (CH algorithm)
set -euo pipefail

PROFILE="${1:?usage: build.sh bike|truck}"
case "$PROFILE" in
    # Profiles shipped inside the OSRM image. OSRM has no truck profile,
    # so trucks use the car profile. If truck limits (weight, streets
    # closed to trucks) ever matter, copy car.lua and edit it.
    bike)  LUA=/opt/bicycle.lua ;;
    truck) LUA=/opt/car.lua ;;
    *) echo "unknown profile: $PROFILE (use bike or truck)" >&2; exit 1 ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OSRM_DIR="$REPO_ROOT/data/osrm"
PBF="$OSRM_DIR/NewYork.osm.pbf"
IMAGE=ghcr.io/project-osrm/osrm-backend

if [ ! -f "$PBF" ]; then
    echo "map file not found: $PBF — run download.sh first" >&2
    exit 1
fi

WORK="$OSRM_DIR/$PROFILE"
mkdir -p "$WORK"
# OSRM names its output files after the input file, so give each profile
# its own copy of the map inside its own directory.
cp -f "$PBF" "$WORK/NewYork.osm.pbf"

docker run --rm -t -v "$WORK:/data" "$IMAGE" osrm-extract -p "$LUA" /data/NewYork.osm.pbf
docker run --rm -t -v "$WORK:/data" "$IMAGE" osrm-contract /data/NewYork.osrm

rm -f "$WORK/NewYork.osm.pbf"
echo "done: $PROFILE graph is in $WORK"
