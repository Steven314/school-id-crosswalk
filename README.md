# School ID Crosswalk

> [!IMPORTANT]  
> This project is being dropped in favor of [Steven314/k12-id-crosswalk](https://github.com/Steven314/k12-id-crosswalk).
> It offers a much faster, simpler, and lighter way to get the essentially the same crosswalk.
> I haven't compared the results for exact consistency, but much of the matching code is the same, so results should be very similar.

When relating educational data from different sources, you often don't have a common identifier.
This makes connecting data difficult.

## Goal

The goal is to connect the identification systems of the below sources with minimal manual intervention.
This differs from the Davenport dataset (linked at the bottom and referenced by the UC-Boulder team) in that it all code will be open source from data collection to final output.

## Scope

This project aims to use several methods to match location information and multiple identification systems.

The resulting crosswalks have only the ID systems and spatial variables.
General information that can be easily joined from the IPEDS directory information table (HD) will not be included so the data has only what you need and nothing extra.

### Higher Education

- [x] [IPEDS](https://nces.ed.gov/ipeds/)
- [x] [CEEB](https://satsuite.collegeboard.org/media/pdf/sat-score-sends-code-list.pdf)
- [x] [US Census Bureau Geospatial data](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html) - We have coordinates from
      IPEDS which can be spatially joined to geometries, as well as the city,
      county, ZIP, and state which can be joined directly.
- [x] [NSC](https://nscresearchcenter.org/workingwithourdata/)

### K-12 Education

- [x] [CEEB](https://satsuite.collegeboard.org/k12-educators/tools-resources/k12-school-code-search)
- [x] [US Census Bureau Geospatial data](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html)
- [x] State-level school IDs — provided by NCES EDGE data via the `state_school_id` field
- [x] [NCES](https://nces.ed.gov/programs/edge/Geographic/SchoolLocations)

At the K-12 level, individual schools are connected to their district (via LEA/NCES district code) in addition to ZIP, county, and state.

### Data Collection

- [x] IPEDS HD table 2009 to 2024 (present)
- [x] US Census Geographies
  - [x] School District
  - [x] ZCTA
  - [x] County
  - [x] State
- [x] CEEB
  - [x] Higher Education Institutions
  - [x] US High Schools
- [x] National Student Clearinghouse (NSC)
- [x] NCES (EDGE)
  - [x] Higher Education Institutions
  - [x] US High Schools
- [x] State-Level School IDs (via NCES EDGE `state_school_id`)

## Tools & Software

- [DuckDB](https://duckdb.org)
  - [FTS Extension](https://duckdb.org/docs/stable/core_extensions/full_text_search) for similarity matching.
  - [Spatial Extension](https://duckdb.org/docs/stable/core_extensions/spatial/overview) for handling shapefiles from the US Census Bureau.
  - [Excel Extension](https://duckdb.org/docs/stable/core_extensions/excel) for loading `.xlsx` data files.
- Python
- [Just](https://github.com/casey/just) for automation and ensuring the results are up-to-date with the code.

## Related Projects

- [`UCBoulder/ceeb_nces_crosswalk`](https://github.com/UCBoulder/ceeb_nces_crosswalk)
- [NORC at University of Chicago, Appendix B (pdf)](https://www.norc.org/content/dam/norc-org/pdfs/HAA%20Phase%201%20Main%20Findings%20Report%20-%20NORC%20-%208.31.2011.pdf)
- Mark Davenport's [presentation](https://uncg.sharepoint.com/:b:/s/dept-10803/EYNEBgCkV2NNovX2c5mVxqwBD0xGGy57gvIvZsqgC6ZKyQ?e=crofWT) from the [NCAIR 2025 Conference](https://nc-air.org/2025-ncair-conference-presentations/).
  - His presentation has some useful information about how the NCES and LEA codes are constructed.
- [LiveBy API](https://docs.liveby.com/api/schools)
  - This seems like a good data source, but it is not free or open source.
- NCES maintains some datasets with [ArcGIS Online](https://data-nces.opendata.arcgis.com/).
  These are available through an [API](https://data-nces.opendata.arcgis.com/pages/use-apis).
  - This would be a good source for updated and historical NCES directory data, but it does not contain any state-level identifiers other than district code.

If I find more, I will add them here.
