#!/usr/bin/env python3
"""
Extract OSM data from flooded areas in Mozambique with contribution timestamps.
Outputs GeoJSON for visualization of rapid coordinated mapping efforts.

Supports incremental updates to reduce API load and processing time.
"""

import requests
import json
from datetime import datetime, timezone, timedelta
import time
import sys
import os

# Overpass API endpoint
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# State file for tracking incremental updates
STATE_FILE = ".osm_update_state.json"

# Initial flood response date (HOT project 39738 created Jan 21, mapping surge Jan 23)
FLOOD_START_DATE = "2026-01-21"

# Bounding boxes for flood-affected areas in Mozambique (Jan 2026)
# Format: (south, west, north, east)
FLOOD_AREAS = {
    "gaza_limpopo": (-24.5, 32.0, -23.0, 35.5),      # Gaza Province - Limpopo River basin
    "sofala_buzi": (-20.5, 33.5, -19.0, 35.0),       # Sofala Province - Buzi River
    "zambezia": (-18.5, 35.0, -16.5, 37.5),          # Zambézia Province
    "maputo_incomati": (-26.0, 32.0, -25.0, 33.0),   # Maputo Province - Incomati/Umbeluzi
}

# Focused bounding box for Chicumbane flood response area (HOT project 39738)
# Project bounds: lat -25.07 to -24.84, lon 33.43 to 33.56
# Expanded slightly to capture surrounding mapping activity
CHICUMBANE_BBOX = (-25.2, 33.3, -24.7, 33.7)

# Combined bounding box for broader area (if needed)
MAIN_BBOX = (-25.5, 32.0, -19.0, 36.0)

def load_state():
    """Load the last update state from file."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {}


def save_state(state):
    """Save the update state to file."""
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def load_existing_geojson(filepath):
    """Load existing GeoJSON file if it exists."""
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return None


def merge_geojson(existing, new_features):
    """
    Merge new features into existing GeoJSON.
    Updates existing features (by osm_id + osm_type) and adds new ones.
    """
    if not existing:
        return {
            "type": "FeatureCollection",
            "features": new_features,
            "metadata": {
                "source": "OpenStreetMap via Overpass API",
                "extracted": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "description": "OSM data from Mozambique flood-affected areas with contribution timestamps"
            }
        }

    # Build index of existing features by osm_id + osm_type
    feature_index = {}
    for f in existing.get("features", []):
        key = (f["properties"].get("osm_id"), f["properties"].get("osm_type"))
        feature_index[key] = f

    # Update or add new features
    updated = 0
    added = 0
    for f in new_features:
        key = (f["properties"].get("osm_id"), f["properties"].get("osm_type"))
        if key in feature_index:
            feature_index[key] = f  # Update existing
            updated += 1
        else:
            feature_index[key] = f  # Add new
            added += 1

    print(f"  Merged: {updated} updated, {added} new features")

    return {
        "type": "FeatureCollection",
        "features": list(feature_index.values()),
        "metadata": {
            "source": "OpenStreetMap via Overpass API",
            "extracted": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "description": "OSM data from Mozambique flood-affected areas with contribution timestamps"
        }
    }


def query_osm_with_metadata(bbox, start_date=None, feature_types=None):
    """
    Query OSM data with full metadata including timestamps, users, and versions.

    Args:
        bbox: (south, west, north, east) bounding box
        start_date: Start date for edits (YYYY-MM-DD format), defaults to 7 days ago
        feature_types: List of feature types to query (default: buildings, highways, waterways)
    """
    if feature_types is None:
        feature_types = ["building", "highway", "waterway", "landuse", "amenity"]

    south, west, north, east = bbox
    if start_date:
        date_filter = f"{start_date}T00:00:00Z"
    else:
        date_filter = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00Z")

    # Build Overpass query with metadata
    # Using 'meta' to get timestamp, version, changeset, user info
    query_parts = []
    for ftype in feature_types:
        query_parts.append(f'  node["{ftype}"](newer:"{date_filter}")({south},{west},{north},{east});')
        query_parts.append(f'  way["{ftype}"](newer:"{date_filter}")({south},{west},{north},{east});')
        query_parts.append(f'  relation["{ftype}"](newer:"{date_filter}")({south},{west},{north},{east});')

    query = f"""
[out:json][timeout:300];
(
{chr(10).join(query_parts)}
);
out meta geom;
"""

    print(f"Querying Overpass API for data modified since {date_filter}...")
    print(f"Bounding box: {bbox}")

    # Retry logic for API timeouts
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.post(OVERPASS_URL, data={"data": query}, timeout=600)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                wait_time = 30 * (attempt + 1)
                print(f"  Timeout, retrying in {wait_time}s (attempt {attempt + 2}/{max_retries})...")
                time.sleep(wait_time)
            else:
                raise
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429 or response.status_code >= 500:
                if attempt < max_retries - 1:
                    wait_time = 60 * (attempt + 1)
                    print(f"  Server error {response.status_code}, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise
            else:
                raise


def osm_to_geojson_with_timestamps(osm_data):
    """
    Convert OSM JSON to GeoJSON with timestamp metadata preserved.
    """
    features = []

    for element in osm_data.get("elements", []):
        feature = {
            "type": "Feature",
            "properties": {
                "osm_id": element.get("id"),
                "osm_type": element.get("type"),
                "timestamp": element.get("timestamp"),
                "version": element.get("version"),
                "changeset": element.get("changeset"),
                "user": element.get("user"),
                "uid": element.get("uid"),
            },
            "geometry": None
        }

        # Add all OSM tags to properties
        if "tags" in element:
            feature["properties"]["tags"] = element["tags"]
            # Also flatten common tags for easier filtering
            for key in ["name", "building", "highway", "waterway", "landuse", "amenity"]:
                if key in element["tags"]:
                    feature["properties"][key] = element["tags"][key]

        # Build geometry based on element type
        if element["type"] == "node":
            feature["geometry"] = {
                "type": "Point",
                "coordinates": [element["lon"], element["lat"]]
            }
        elif element["type"] == "way":
            if "geometry" in element:
                coords = [[pt["lon"], pt["lat"]] for pt in element["geometry"]]
                # Check if it's a closed way (polygon) or line
                if coords[0] == coords[-1] and len(coords) > 3:
                    feature["geometry"] = {
                        "type": "Polygon",
                        "coordinates": [coords]
                    }
                else:
                    feature["geometry"] = {
                        "type": "LineString",
                        "coordinates": coords
                    }
        elif element["type"] == "relation":
            # Simplify relations - just use bounds for now
            if "bounds" in element:
                bounds = element["bounds"]
                feature["geometry"] = {
                    "type": "Polygon",
                    "coordinates": [[
                        [bounds["minlon"], bounds["minlat"]],
                        [bounds["maxlon"], bounds["minlat"]],
                        [bounds["maxlon"], bounds["maxlat"]],
                        [bounds["minlon"], bounds["maxlat"]],
                        [bounds["minlon"], bounds["minlat"]]
                    ]]
                }

        if feature["geometry"]:
            features.append(feature)

    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "source": "OpenStreetMap via Overpass API",
            "extracted": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "description": "OSM data from Mozambique flood-affected areas with contribution timestamps"
        }
    }


def analyze_contributions(geojson):
    """
    Analyze the contribution patterns to show coordinated mapping.
    """
    features = geojson["features"]

    if not features:
        return {"error": "No features found"}

    # Aggregate by user
    users = {}
    # Aggregate by date
    dates = {}
    # Aggregate by hour
    hours = {}

    for f in features:
        props = f["properties"]

        user = props.get("user", "unknown")
        users[user] = users.get(user, 0) + 1

        ts = props.get("timestamp")
        if ts:
            date = ts[:10]
            dates[date] = dates.get(date, 0) + 1

            hour = ts[:13]
            hours[hour] = hours.get(hour, 0) + 1

    # Sort and get top contributors
    top_users = sorted(users.items(), key=lambda x: -x[1])[:20]
    date_series = sorted(dates.items())

    return {
        "total_features": len(features),
        "unique_contributors": len(users),
        "top_contributors": top_users,
        "edits_by_date": date_series,
        "date_range": {
            "earliest": min(dates.keys()) if dates else None,
            "latest": max(dates.keys()) if dates else None
        }
    }


def main():
    # Parse command line arguments
    full_refresh = "--full" in sys.argv

    print("=" * 60)
    print("Mozambique Flood Area OSM Data Extractor")
    if full_refresh:
        print("Mode: FULL REFRESH")
    else:
        print("Mode: INCREMENTAL UPDATE")
    print("=" * 60)

    output_file = "mozambique_flood_mapping.geojson"

    # Determine the start date for the query
    state = load_state()
    existing_geojson = None

    if full_refresh:
        # Full refresh: start from flood response date
        start_date = FLOOD_START_DATE
        print(f"\nFull refresh from {start_date}")
    else:
        # Incremental: check last update time
        last_update = state.get("last_update")
        if last_update:
            # Use last update time, but go back 1 hour to catch any edge cases
            start_date = last_update[:10]  # Just the date part
            existing_geojson = load_existing_geojson(output_file)
            if existing_geojson:
                print(f"\nIncremental update since {start_date}")
                print(f"  Existing features: {len(existing_geojson.get('features', []))}")
            else:
                # No existing file, do full refresh
                start_date = FLOOD_START_DATE
                print(f"\nNo existing data found, doing full refresh from {start_date}")
        else:
            # No state file, do full refresh
            start_date = FLOOD_START_DATE
            print(f"\nNo previous state found, doing full refresh from {start_date}")

    # Extract data from Chicumbane flood response area (HOT project 39738)
    try:
        osm_data = query_osm_with_metadata(
            CHICUMBANE_BBOX,
            start_date=start_date,
            feature_types=["building", "highway", "waterway"]
        )
    except requests.exceptions.RequestException as e:
        print(f"Error querying Overpass API: {e}")
        return

    print(f"\nReceived {len(osm_data.get('elements', []))} elements from OSM")

    # Convert to GeoJSON
    new_geojson = osm_to_geojson_with_timestamps(osm_data)

    # Merge with existing if incremental
    if existing_geojson and not full_refresh:
        geojson = merge_geojson(existing_geojson, new_geojson.get("features", []))
    else:
        geojson = new_geojson

    # Save GeoJSON
    with open(output_file, "w") as f:
        json.dump(geojson, f, indent=2)
    print(f"\nSaved GeoJSON to: {output_file}")

    # Update state
    state["last_update"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    state["feature_count"] = len(geojson.get("features", []))
    save_state(state)
    print(f"Updated state file: {STATE_FILE}")

    # Analyze contributions
    stats = analyze_contributions(geojson)

    # Save stats
    stats_file = "mozambique_mapping_stats.json"
    with open(stats_file, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"Saved statistics to: {stats_file}")

    # Print summary
    print("\n" + "=" * 60)
    print("CONTRIBUTION ANALYSIS")
    print("=" * 60)
    print(f"Total features mapped: {stats.get('total_features', 0)}")
    print(f"Unique contributors: {stats.get('unique_contributors', 0)}")

    if stats.get("date_range", {}).get("earliest"):
        print(f"Date range: {stats['date_range']['earliest']} to {stats['date_range']['latest']}")

    print("\nTop 10 contributors:")
    for user, count in stats.get("top_contributors", [])[:10]:
        print(f"  {user}: {count} edits")

    print("\nEdits by date (last 10 days):")
    for date, count in stats.get("edits_by_date", [])[-10:]:
        bar = "█" * min(count // 10, 50)
        print(f"  {date}: {count:5d} {bar}")

    print("\n" + "=" * 60)
    print("Done! Open mozambique_flood_mapping.geojson in QGIS, kepler.gl,")
    print("or geojson.io to visualize the coordinated mapping effort.")
    print("=" * 60)


if __name__ == "__main__":
    main()
