# Mozambique Flood Response - OSM Mapping Visualization

Interactive visualization of coordinated OpenStreetMap mapping efforts in response to the January 2026 Mozambique floods and subsequent Tropical Cyclone Gezani.

## Live Demo

**[View the interactive map](https://cgiovando.github.io/osm-moz-jan2026/)**

## Overview

In January 2026, severe flooding affected over 600,000 people in Mozambique, with Gaza, Sofala, and Maputo provinces hit hardest. In February, Tropical Cyclone Gezani brought additional flooding to the region. This visualization shows the rapid coordinated mapping response by the OpenStreetMap community across multiple HOT Tasking Manager project areas.

### Key Statistics

- **278,000+** features mapped
- **1,117+** contributors
- **9** HOT project areas across Gaza Province
- Date range: January 23 - March 28, 2026

## HOT Project Areas

| Project | ID | Status |
|---------|----|--------|
| Chicumbane | [#39738](https://tasks.hotosm.org/projects/39738) | ARCHIVED |
| Guija 01 | [#40233](https://tasks.hotosm.org/projects/40233) | ARCHIVED |
| Chinhacanine | [#40266](https://tasks.hotosm.org/projects/40266) | ARCHIVED |
| Guija 02 | [#40299](https://tasks.hotosm.org/projects/40299) | ARCHIVED |
| Massingir | [#40365](https://tasks.hotosm.org/projects/40365) | ARCHIVED |
| Cidade De Xai Xai | [#40662](https://tasks.hotosm.org/projects/40662) | ARCHIVED |
| Massingir-01 | [#40959](https://tasks.hotosm.org/projects/40959) | ARCHIVED |
| Chongoene01 | [#42345](https://tasks.hotosm.org/projects/42345) | ARCHIVED |
| TC GEZANI 26, Massinga 1 | [#42873](https://tasks.hotosm.org/projects/42873) | PUBLISHED |

## Features

- **Scrollable Timeline**: Detailed bar chart showing mapping activity by hour with day separators and horizontal scrolling
- **Animated Playback**: Watch the mapping effort unfold over time with new features flashing yellow as they appear
- **Dynamic Stats Overlay**: Live counter showing features mapped, with date/time display during animation
- **Top Contributors**: Expandable list showing top 20 OSM mappers and their contribution counts
- **Zoom-Adaptive Buildings**: Buildings shown as dots at low zoom, detailed polygons at high zoom - visible at all zoom levels
- **Layer Toggles**: Show/hide buildings, highways, waterways, and HOT project boundaries
- **Basemap Switcher**: Choose between CARTO Dark, CARTO Light, OSM Humanitarian, or ESRI Satellite
- **Overview Map**: Mini locator map showing current viewport location within Mozambique
- **HOT Project Boundaries**: Overlay of Humanitarian OpenStreetMap Team Tasking Manager projects
- **URL Hash**: Shareable URLs with zoom/lat/lng parameters

## Technology

- **[MapLibre GL JS](https://maplibre.org/)**: Open-source map rendering
- **[PMTiles](https://github.com/protomaps/PMTiles)**: Efficient single-file tile archive format
- **[CARTO Dark Basemap](https://carto.com/basemaps/)**: Background map tiles

## Data Sources

- **OSM Features**: Extracted via [Overpass API](https://overpass-api.de/) with contribution metadata (timestamps, users, changesets)
- **HOT Projects**: Fetched from [HOT Tasking Manager API](https://tasks.hotosm.org/)

### HOT Projects

HOT Tasking Manager projects are discovered dynamically by searching for Mozambique flood/cyclone/emergency projects created after January 15, 2026. Active, published, and archived projects are all included. New projects are automatically picked up, and OSM data is extracted from each project's bounding area independently.

## Data Updates

Daily automatic updates have been stopped as the flood response phase has concluded. The data reflects OSM mapping activity from January 23 through March 28, 2026.

If you need a data refresh with more recent OSM edits, please [submit an issue](../../issues/new) requesting updated data. The update workflow can still be triggered manually.

## Files

| File | Description |
|------|-------------|
| `index.html` | Interactive web visualization (MapLibre GL JS) |
| `mozambique_flood_mapping.pmtiles` | Building polygons as PMTiles for efficient tile serving |
| `building_centroids.pmtiles` | Pre-computed building centroids as PMTiles for fast low-zoom rendering |
| `highways_waterways.geojson` | Highway and waterway line features (GeoJSON) |
| `hot_projects.geojson` | HOT Tasking Manager project boundaries |
| `mozambique_mapping_stats.json` | Aggregated mapping statistics (hourly by type, daily, contributors) |
| `extract_mozambique_osm.py` | Python script to extract OSM data (supports incremental updates) |
| `fetch_hot_projects.py` | Python script to dynamically discover HOT project boundaries |
| `compute_centroids.py` | Python script to generate building centroids and extract highways/waterways |
| `.github/workflows/update-data.yml` | GitHub Actions workflow for automated daily updates |

## References

- [UN OCHA Flash Update - Mozambique Floods](https://reliefweb.int/report/mozambique/flash-update-no5-heavy-rains-and-floods-central-and-southern-mozambique-26-january-2026)
- [HOT Tasking Manager - Mozambique Projects](https://tasks.hotosm.org/explore?text=mozambique)
- [OpenStreetMap](https://www.openstreetmap.org/)

## AI-assisted development

> This project was developed with significant assistance from AI coding tools.

- **[Claude Code](https://claude.ai/claude-code)** (Anthropic) — code generation, architecture, debugging, and documentation
- All functionality has been tested and verified to work as intended
- Features and infrastructure choices have been reviewed and approved by the maintainer

This disclosure follows emerging best practices for transparency in AI-assisted software development.

## License

Data: [ODbL](https://opendatacommons.org/licenses/odbl/) (OpenStreetMap data)
Code: MIT
