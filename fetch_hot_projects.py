#!/usr/bin/env python3
"""
Fetch HOT Tasking Manager project boundaries for Mozambique flood response.
"""

import requests
import json

HOT_API = "https://tasking-manager-tm4-production-api.hotosm.org/api/v2"

# Key flood-related projects
FLOOD_PROJECTS = [
    39738,  # Mozambique Floods, Chicumbane (URGENT - current)
    18375,  # Mapping for Mozambique: Buzi 1 (HIGH)
    18385,  # Mapping for Mozambique: Buzi 7 (HIGH)
]

def fetch_project(project_id):
    """Fetch project details including AOI boundary."""
    url = f"{HOT_API}/projects/{project_id}/?as_file=false&abbreviated=false"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()

def main():
    features = []

    for pid in FLOOD_PROJECTS:
        print(f"Fetching project {pid}...")
        try:
            data = fetch_project(pid)

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
                "geometry": data["areaOfInterest"]
            }
            features.append(feature)
            print(f"  - {data['projectInfo']['name']}: {data['percentMapped']}% mapped")

        except Exception as e:
            print(f"  Error fetching project {pid}: {e}")

    # Create GeoJSON FeatureCollection
    geojson = {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "source": "HOT Tasking Manager API",
            "description": "Mozambique flood response project boundaries"
        }
    }

    # Save to file
    output_file = "hot_projects.geojson"
    with open(output_file, "w") as f:
        json.dump(geojson, f, indent=2)

    print(f"\nSaved {len(features)} project boundaries to {output_file}")

if __name__ == "__main__":
    main()
