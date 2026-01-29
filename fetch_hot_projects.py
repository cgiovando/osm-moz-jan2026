#!/usr/bin/env python3
"""
Fetch HOT Tasking Manager project boundaries for Mozambique flood response.
Dynamically discovers projects by searching for Mozambique-related keywords.
"""

import requests
import json
from datetime import datetime, timezone

HOT_API = "https://tasking-manager-tm4-production-api.hotosm.org/api/v2"

# Mozambique bounding box (approximate)
MOZAMBIQUE_BBOX = {
    "min_lon": 30.0,
    "min_lat": -27.0,
    "max_lon": 41.0,
    "max_lat": -10.0
}

# Search terms for finding flood-related projects
SEARCH_TERMS = [
    "mozambique flood",
    "mocambique flood",
    "mozambique cyclone",
    "mozambique emergency",
    "mozambique disaster"
]

# Keywords that must appear in project name/description for inclusion
FLOOD_KEYWORDS = ["flood", "inundacao", "inundação", "cyclone", "ciclone", "emergency", "disaster"]

# Project statuses to include
VALID_STATUSES = ["PUBLISHED", "ACTIVE"]

# Minimum creation date (only include projects created after the flood event)
# January 2026 Mozambique floods started around January 20, 2026
MIN_CREATION_DATE = "2026-01-15"


def search_projects(search_text):
    """Search for projects matching the search text."""
    url = f"{HOT_API}/projects/"
    params = {
        "textSearch": search_text,
        "orderBy": "id",
        "orderByType": "DESC",
        "page": 1
    }

    all_projects = []

    while True:
        print(f"  Searching page {params['page']} for '{search_text}'...")
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            results = data.get("results", [])
            if not results:
                break

            all_projects.extend(results)

            # Check if there are more pages
            pagination = data.get("pagination", {})
            if params["page"] >= pagination.get("pages", 1):
                break

            params["page"] += 1

        except Exception as e:
            print(f"  Error searching: {e}")
            break

    return all_projects


def is_in_mozambique(geometry):
    """Check if a geometry intersects with Mozambique bbox (simple check)."""
    if not geometry:
        return False

    # Extract all coordinates from the geometry
    def get_all_coords(geom):
        coords = []
        if geom["type"] == "Point":
            coords.append(geom["coordinates"])
        elif geom["type"] in ["LineString", "MultiPoint"]:
            coords.extend(geom["coordinates"])
        elif geom["type"] in ["Polygon", "MultiLineString"]:
            for ring in geom["coordinates"]:
                coords.extend(ring)
        elif geom["type"] == "MultiPolygon":
            for polygon in geom["coordinates"]:
                for ring in polygon:
                    coords.extend(ring)
        return coords

    coords = get_all_coords(geometry)
    if not coords:
        return False

    # Check if any coordinate is within Mozambique bbox
    for coord in coords:
        lon, lat = coord[0], coord[1]
        if (MOZAMBIQUE_BBOX["min_lon"] <= lon <= MOZAMBIQUE_BBOX["max_lon"] and
            MOZAMBIQUE_BBOX["min_lat"] <= lat <= MOZAMBIQUE_BBOX["max_lat"]):
            return True

    return False


def fetch_project_details(project_id):
    """Fetch full project details including AOI boundary."""
    url = f"{HOT_API}/projects/{project_id}/?as_file=false&abbreviated=false"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def is_flood_related(project_data):
    """Check if project is flood/disaster related based on name and description."""
    name = project_data.get("projectInfo", {}).get("name", "").lower()
    description = project_data.get("projectInfo", {}).get("shortDescription", "").lower()
    text = name + " " + description

    for keyword in FLOOD_KEYWORDS:
        if keyword.lower() in text:
            return True
    return False


def is_recent_project(project_data):
    """Check if project was created after MIN_CREATION_DATE."""
    created = project_data.get("created", "")
    if not created:
        return False
    return created >= MIN_CREATION_DATE


def main():
    print("=" * 60)
    print("HOT Tasking Manager Project Fetcher")
    print("Searching for Mozambique flood-related projects...")
    print("=" * 60)

    # Collect unique project IDs from all search terms
    found_projects = {}

    for term in SEARCH_TERMS:
        print(f"\nSearching for: '{term}'")
        projects = search_projects(term)
        for proj in projects:
            pid = proj.get("projectId")
            if pid and pid not in found_projects:
                found_projects[pid] = proj

    print(f"\nFound {len(found_projects)} unique projects from search")

    # Filter and fetch full details
    features = []
    for pid, proj_summary in found_projects.items():
        status = proj_summary.get("status", "")

        # Skip if not in valid status
        if status not in VALID_STATUSES:
            print(f"  Skipping project {pid}: status={status}")
            continue

        print(f"\nFetching details for project {pid}...")
        try:
            data = fetch_project_details(pid)

            # Check if project is in Mozambique
            aoi = data.get("areaOfInterest")
            if not is_in_mozambique(aoi):
                print(f"  Skipping project {pid}: not in Mozambique bbox")
                continue

            # Check if project is recent enough
            if not is_recent_project(data):
                print(f"  Skipping project {pid}: created before {MIN_CREATION_DATE}")
                continue

            # Check if project is flood/disaster related
            if not is_flood_related(data):
                print(f"  Skipping project {pid}: not flood-related")
                continue

            # Create a feature for this project
            feature = {
                "type": "Feature",
                "properties": {
                    "projectId": data["projectId"],
                    "name": data["projectInfo"]["name"],
                    "status": data["status"],
                    "priority": data.get("projectPriority", "MEDIUM"),
                    "created": data["created"],
                    "percentMapped": data["percentMapped"],
                    "percentValidated": data["percentValidated"],
                    "totalContributors": data.get("totalContributors", 0),
                    "description": data["projectInfo"].get("shortDescription", ""),
                    "url": f"https://tasks.hotosm.org/projects/{pid}"
                },
                "geometry": aoi
            }
            features.append(feature)

            name = data['projectInfo']['name']
            mapped = data['percentMapped']
            priority = data.get('projectPriority', 'MEDIUM')
            print(f"  + {name}")
            print(f"    Priority: {priority}, Mapped: {mapped}%")

        except Exception as e:
            print(f"  Error fetching project {pid}: {e}")

    # Sort by project ID (newest first)
    features.sort(key=lambda f: f["properties"]["projectId"], reverse=True)

    # Create GeoJSON FeatureCollection
    geojson = {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "source": "HOT Tasking Manager API",
            "description": "Mozambique flood response project boundaries",
            "updated": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "project_count": len(features)
        }
    }

    # Save to file
    output_file = "hot_projects.geojson"
    with open(output_file, "w") as f:
        json.dump(geojson, f, indent=2)

    print("\n" + "=" * 60)
    print(f"Saved {len(features)} project boundaries to {output_file}")
    print("=" * 60)


if __name__ == "__main__":
    main()
