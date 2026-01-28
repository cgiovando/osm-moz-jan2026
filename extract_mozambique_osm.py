#!/usr/bin/env python3
"""
Extract OSM data from flooded areas in Mozambique with contribution timestamps.
Outputs GeoJSON for visualization of rapid coordinated mapping efforts.
"""

import requests
import json
from datetime import datetime, timedelta
import time

# Overpass API endpoint
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

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
        date_filter = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00Z")

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

    response = requests.post(OVERPASS_URL, data={"data": query}, timeout=600)
    response.raise_for_status()

    return response.json()


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
            "extracted": datetime.utcnow().isoformat() + "Z",
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
    print("=" * 60)
    print("Mozambique Flood Area OSM Data Extractor")
    print("Extracting recent mapping contributions with timestamps")
    print("=" * 60)

    # Extract data from Chicumbane flood response area (HOT project 39738)
    # Starting from Jan 21, 2026 when HOT project was created
    # (flood response mapping surge began Jan 23)
    try:
        osm_data = query_osm_with_metadata(
            CHICUMBANE_BBOX,
            start_date="2026-01-21",
            feature_types=["building", "highway", "waterway"]
        )
    except requests.exceptions.RequestException as e:
        print(f"Error querying Overpass API: {e}")
        return

    print(f"\nReceived {len(osm_data.get('elements', []))} elements from OSM")

    # Convert to GeoJSON
    geojson = osm_to_geojson_with_timestamps(osm_data)

    # Save GeoJSON
    output_file = "mozambique_flood_mapping.geojson"
    with open(output_file, "w") as f:
        json.dump(geojson, f, indent=2)
    print(f"\nSaved GeoJSON to: {output_file}")

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
