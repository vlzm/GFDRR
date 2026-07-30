---
title: "Set up OSRM"
weight: 5
---

# OSRM Setup — New York City (Ubuntu + Docker)

OSRM (Open Source Routing Machine) answers routing questions over the
OpenStreetMap road network: travel time and distance between two points,
and time/distance matrices for many points at once. In this project it
serves bike trips between Citi Bike stations now, and truck trips for
rebalancing in a later iteration.

OSRM is not installed in the usual sense — every step runs the prebuilt
image `ghcr.io/project-osrm/osrm-backend` through Docker. The process has
four phases:

1. **Download** the New York map (`.osm.pbf` file) — once, shared by all profiles
2. **Extract** — turn the raw map into a graph (`osrm-extract`)
3. **Contract** — precompute shortcuts for fast routing (`osrm-contract`)
4. **Run** — start the server (`osrm-routed`)

A "profile" is a set of rules for one vehicle type: which roads it may
use and how fast it moves on each road type. The profile is baked into
the graph at build time, so bikes and trucks need two separate graphs
and two separate servers. The scripts in `scripts/osrm/` handle both:

| profile | OSRM rules file | graph directory   | server port |
|---------|-----------------|-------------------|-------------|
| `bike`  | `/opt/bicycle.lua` (in the image) | `data/osrm/bike/`  | 5000 |
| `truck` | `/opt/car.lua` (in the image)     | `data/osrm/truck/` | 5001 |

OSRM ships no truck profile, so trucks use the car profile. If truck
limits (weight, streets closed to trucks) ever matter, copy `car.lua`,
edit it, and pass the copy to `osrm-extract`.

Running `osrm-routed` without an `--algorithm` flag uses the default
algorithm, CH (Contraction Hierarchies), so the build step uses
`osrm-contract`. For the MLD pipeline instead, replace `osrm-contract`
with `osrm-partition` + `osrm-customize` and add `--algorithm mld` to
`osrm-routed`.

---

## Step 0. Install Docker (once, needs sudo)

```bash
sudo apt-get update
sudo apt-get install -y docker.io
sudo usermod -aG docker $USER
```

Log out and back in (or run `newgrp docker` in the current shell) so the
group change takes effect, then check:

```bash
docker run --rm hello-world
```

## Step 1. Download the New York map

```bash
./scripts/osrm/download.sh
```

This saves `data/osrm/NewYork.osm.pbf` (~145 MB) from BBBike. The BBBike
"NewYork" extract covers the metro area: longitude −74.36 … −73.67,
latitude 40.48 … 40.96. That includes Jersey City and Hoboken on the New
Jersey side of the Hudson, where Citi Bike also has stations.

Do not swap this for the Geofabrik "new-york" extract: that file covers
the **state** of New York only, so the New Jersey stations would fall
outside the map and snap to wrong roads.

## Step 2. Build the bike graph

```bash
./scripts/osrm/build.sh bike
```

This runs `osrm-extract` with the bicycle profile and then
`osrm-contract`, both inside Docker. On success `data/osrm/bike/`
contains many `NewYork.osrm.*` files — this is expected. Takes a few
minutes and a few GB of RAM.

## Step 3. Start the bike server

```bash
./scripts/osrm/serve.sh bike        # foreground, stop with Ctrl+C
./scripts/osrm/serve.sh bike -d     # background, stop with: docker stop osrm-bike
```

The server listens on `http://127.0.0.1:5000`. In foreground mode the
terminal appears to hang — that is normal, the server occupies it.

## Step 4. Test

Coordinates are in `longitude,latitude` order. A bike route from Times
Square to Wall Street:

```bash
curl "http://127.0.0.1:5000/route/v1/bike/-73.9857,40.7580;-74.0088,40.7061"
```

A JSON response with `"code":"Ok"` means it works. `duration` is seconds,
`distance` is meters.

A duration/distance matrix for many points in one request (this is the
call a simulation over all stations wants — the server is started with
`--max-table-size 10000`, so up to 10 000 points fit):

```bash
curl "http://127.0.0.1:5000/table/v1/bike/-73.9857,40.7580;-74.0088,40.7061;-73.9442,40.6782?annotations=duration,distance"
```

Note: the vehicle name in the URL (`/route/v1/bike/...`) is only a label;
the answer always comes from the profile baked into the graph the server
loaded.

## Step 5. Use OSRM in the pipeline

The simulation measures distances and travel times between facilities
through one object, `routes` (Notations.md §13). Its `routing_mode` picks
how:

- `haversine` (default) — straight-line formula, no server needed
- `osrm` — road-network distance and riding time from this server

From the terminal:

```bash
python app/runner.py --run-name my_run --routing osrm
# --osrm-url http://127.0.0.1:5000 is the default
```

From Python or a notebook:

```python
graph_data = build_resolved(raw_data, routing_mode="osrm")
```

With `routing_mode="osrm"` the loader fetches the full
facility-to-facility distance/duration table in one `/table` request while
building `ResolvedModelData` (a few seconds for ~2 300 facilities); after
that the simulation makes no network calls. The server must be running
first (Step 3), or the loader raises a connection error.

What changes with the mode: the trip distances in the run artifact and the
wide journal, and the travel-time estimate for a redirect pair with no OD
entry. Historical trip durations (the OD matrix) stay historical in both
modes.

## Later: the truck server (rebalancing iteration)

Same two commands, different argument — the map from Step 1 is reused:

```bash
./scripts/osrm/build.sh truck
./scripts/osrm/serve.sh truck       # port 5001
curl "http://127.0.0.1:5001/route/v1/truck/-73.9857,40.7580;-74.0088,40.7061"
```

Both servers can run at the same time: bike on 5000, truck on 5001.

---

## Good to know

**`.osrm` is a base name, not a single file.** `osrm-routed
/data/NewYork.osrm` uses the whole `NewYork.osrm.*` set. When moving
things, move the whole directory.

**Updating the map.** BBBike refreshes the extract weekly. Re-run Step 1
(overwrites the old file), then re-run Step 2 for each profile you use.

**Troubleshooting:**

- `osrm-extract` killed — out of RAM. Close memory-hungry programs; the
  build needs a few GB free.
- Port already in use — edit the port in `scripts/osrm/serve.sh`, or stop
  whatever holds it (`ss -ltnp | grep 5000`).
- `permission denied` on `docker run` — the group change from Step 0 is
  not active yet; log out and back in, or run `newgrp docker`.
