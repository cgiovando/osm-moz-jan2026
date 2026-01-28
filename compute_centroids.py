#!/usr/bin/env python3
"""
Compute centroids for building features to enable fast rendering at low zoom levels.
Outputs a lightweight GeoJSON with Point geometries and all original properties.
"""

import json
import sys
from pathlib import Path


def compute_centroid(geometry):
    """
    Compute the centroid of a geometry.
    Returns [longitude, latitude] coordinates.
    """
    geom_type = geometry.get("type")
    coords = geometry.get("coordinates")

    if geom_type == "Point":
        return coords

    elif geom_type == "Polygon":
        # Use the outer ring (first array of coordinates)
        ring = coords[0]
        if not ring:
            return None
        sum_x = sum(pt[0] for pt in ring)
        sum_y = sum(pt[1] for pt in ring)
        return [sum_x / len(ring), sum_y / len(ring)]

    elif geom_type == "MultiPolygon":
        # Use the first polygon's outer ring
        if coords and coords[0] and coords[0][0]:
            ring = coords[0][0]
            sum_x = sum(pt[0] for pt in ring)
            sum_y = sum(pt[1] for pt in ring)
            return [sum_x / len(ring), sum_y / len(ring)]
        return None

    elif geom_type == "LineString":
        # Use midpoint
        if coords:
            mid_idx = len(coords) // 2
            return coords[mid_idx]
        return None

    elif geom_type == "MultiLineString":
        # Use first line's midpoint
        if coords and coords[0]:
            line = coords[0]
            mid_idx = len(line) // 2
            return line[mid_idx]
        return None

    return None


def process_geojson(input_path, output_path, feature_filter=None):
    """
    Process a GeoJSON file and output centroids for filtered features.

    Args:
        input_path: Path to input GeoJSON file
        output_path: Path to output centroids GeoJSON file
        feature_filter: Function to filter features (default: buildings only)
    """
    print(f"Reading {input_path}...")

    with open(input_path, 'r') as f:
        data = json.load(f)

    features = data.get("features", [])
    print(f"Total features: {len(features)}")

    # Default filter: buildings only
    if feature_filter is None:
        feature_filter = lambda f: f.get("properties", {}).get("building") is not None

    # Process features
    centroids = []
    skipped = 0

    for feature in features:
        # Apply filter
        if not feature_filter(feature):
            continue

        geometry = feature.get("geometry")
        if not geometry:
            skipped += 1
            continue

        # Compute centroid
        centroid_coords = compute_centroid(geometry)
        if not centroid_coords:
            skipped += 1
            continue

        # Create point feature with same properties
        centroid_feature = {
            "type": "Feature",
            "properties": feature.get("properties", {}),
            "geometry": {
                "type": "Point",
                "coordinates": centroid_coords
            }
        }
        centroids.append(centroid_feature)

    print(f"Computed {len(centroids)} centroids, skipped {skipped} features")

    # Create output GeoJSON
    output_data = {
        "type": "FeatureCollection",
        "features": centroids,
        "metadata": {
            "source": "Computed from " + str(input_path),
            "description": "Building centroids for low-zoom rendering",
            "feature_count": len(centroids)
        }
    }

    # Write output
    print(f"Writing {output_path}...")
    with open(output_path, 'w') as f:
        json.dump(output_data, f)

    # Report file sizes
    input_size = Path(input_path).stat().st_size / (1024 * 1024)
    output_size = Path(output_path).stat().st_size / (1024 * 1024)
    reduction = (1 - output_size / input_size) * 100

    print(f"Input size:  {input_size:.2f} MB")
    print(f"Output size: {output_size:.2f} MB")
    print(f"Size reduction: {reduction:.1f}%")

    return len(centroids)


def main():
    # Default paths
    input_file = "mozambique_flood_mapping.geojson"
    output_file = "building_centroids.geojson"

    # Allow command line arguments
    if len(sys.argv) >= 2:
        input_file = sys.argv[1]
    if len(sys.argv) >= 3:
        output_file = sys.argv[2]

    print("=" * 60)
    print("Building Centroid Generator")
    print("=" * 60)

    count = process_geojson(input_file, output_file)

    print("=" * 60)
    print(f"Done! Generated {count} building centroids.")
    print(f"Output: {output_file}")
    print("=" * 60)


if __name__ == "__main__":
    main()
