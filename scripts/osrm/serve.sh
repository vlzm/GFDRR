#!/usr/bin/env bash
# Start the OSRM routing server for one travel profile.
#
# Usage: ./serve.sh bike|truck [-d]
#
#   bike  -> http://127.0.0.1:5000
#   truck -> http://127.0.0.1:5001
#
# By default the server runs in the foreground and holds the terminal;
# stop it with Ctrl+C. Pass -d to run it in the background instead;
# stop that with: docker stop osrm-bike (or osrm-truck).
set -euo pipefail

PROFILE="${1:?usage: serve.sh bike|truck [-d]}"
case "$PROFILE" in
    bike)  PORT=5000 ;;
    truck) PORT=5001 ;;
    *) echo "unknown profile: $PROFILE (use bike or truck)" >&2; exit 1 ;;
esac

MODE="-t -i"
if [ "${2:-}" = "-d" ]; then
    MODE="-d"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORK="$REPO_ROOT/data/osrm/$PROFILE"

if [ ! -f "$WORK/NewYork.osrm.hsgr" ]; then
    echo "graph not found in $WORK — run build.sh $PROFILE first" >&2
    exit 1
fi

# --max-table-size 10000 raises the limit on /table requests (default is
# 100 points), so a duration matrix over all Citi Bike stations fits in
# one request.
exec docker run --rm $MODE -p "$PORT:5000" -v "$WORK:/data" \
    --name "osrm-$PROFILE" ghcr.io/project-osrm/osrm-backend \
    osrm-routed --max-table-size 10000 /data/NewYork.osrm
