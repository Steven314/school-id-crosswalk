# School ID Crosswalk

When relating educational data from different sources, you often don't have a common identifier.
This makes connecting data difficult.

## Scope

This project aims to use several methods to match location information and multiple identification systems.

### Higher Education

- [ ] [IPEDS](https://nces.ed.gov/ipeds/)
- [ ] [CEEB](https://satsuite.collegeboard.org/media/pdf/sat-score-sends-code-list.pdf)
- [ ] [US Census Bureau Geospatial data](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html)
- [ ] [NCES](https://nces.ed.gov/programs/edge/Geographic/SchoolLocations)

### K-12 Education

- [ ] [CEEB](https://satsuite.collegeboard.org/k12-educators/tools-resources/k12-school-code-search)
- [ ] [US Census Bureau Geospatial data](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html)
- [ ] state-level identification systems
  - namely Michigan and Ohio at first.
- [ ] [NCES](https://nces.ed.gov/programs/edge/Geographic/SchoolLocations)

At the K-12 level, I would like to be able to also connect individuals schools to their district in addition to ZIP, county, and state.

### Data Collection

- [X] IPEDS HD table 2009 to 2024 (present)
- [X] US Census Geographies
  - [X] School District
  - [X] ZCTA
  - [X] County
  - [X] State
- [ ] CEEB
  - [ ] Higher Education Institutions
  - [ ] US High Schools
- [X] National Student Clearinghouse (NSC)
- [X] NCES (EDGE)
  - [X] Higher Education Institutions
  - [X] US High Schools
- [ ] State-Level ID Systems
  - [ ] Indiana
  - [ ] Michigan
  - [ ] Ohio

## Tools & Software

- [DuckDB](https://duckdb.org)
  - [FTS Extension](https://duckdb.org/docs/stable/core_extensions/full_text_search) for similarity matching.
  - [Spatial Extension](https://duckdb.org/docs/stable/core_extensions/spatial/overview) for handling shapefiles from the US Census Bureau.
- R
- Python
- [Make](https://www.gnu.org/software/make/) for automation and ensuring the results are up-to-date with the code.

## Related Projects

- [`UCBoulder/ceeb_nces_crosswalk`](https://github.com/UCBoulder/ceeb_nces_crosswalk)
- [NORC at University of Chicago, Appendix B (pdf)](https://www.norc.org/content/dam/norc-org/pdfs/HAA%20Phase%201%20Main%20Findings%20Report%20-%20NORC%20-%208.31.2011.pdf)

If I find more, I will add them here.
