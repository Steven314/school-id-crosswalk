import os
from typing import Any, List

import duckdb
import polars as pl

from utils.duckdb import attach_db_dir


def create_table(duck: duckdb.DuckDBPyConnection, path: str, table_name: str):
    with open(path) as f:
        duck.sql(f"CREATE OR REPLACE TABLE {table_name} AS ({f.read()})")


def create_school_tables(duck: duckdb.DuckDBPyConnection):
    create_table(
        duck,
        os.path.join("crosswalking", "sql", "hd.sql"),
        "universities.ipeds_hd",
    )
    create_table(
        duck,
        os.path.join("crosswalking", "sql", "ceeb_university.sql"),
        "universities.ceeb_university",
    )
    create_table(
        duck,
        os.path.join("crosswalking", "sql", "nsc.sql"),
        "universities.nsc_university",
    )

#####


duck: duckdb.DuckDBPyConnection = duckdb.connect(  # type: ignore
    "crosswalking/universities.duckdb"
)


for file in ["ceeb.duckdb", "ipeds.duckdb", "geography.duckdb", "nsc.duckdb"]:
    if file.endswith(".duckdb"):
        duck.execute(
            f"ATTACH IF NOT EXISTS '{os.path.join('clean-data', file)}'"
        )

create_school_tables(duck)

# exact matches
exact_matches = duck.sql(
    "select * "
    "from ipeds_hd a "
    "inner join ceeb_university b "
    "on (lower(a.name) = lower(b.name) and a.state = b.state)"
)

# inner join match quality
n_ipeds = len(duck.sql("from ipeds_hd").pl())
n_ceebs = len(duck.sql("from ceeb_university").pl())
n_exact = len(exact_matches.pl())
print(n_exact / min(n_ipeds, n_ceebs))

# There are fewer university CEEB codes than there are IPEDS codes. That gives
# us a decent first pass at matches. There are also only a few duplicates and
# this stems from having some duplicate values in the IPEDS data to begin.

print(f"Number of Exact Matches by Name and State: {n_exact}")
print(
    f"Number of Distinct Exact Matches: {len(exact_matches.select('name', 'state').distinct().pl())}"
)
print(
    f"Number of Distinct Exact Names: {len(exact_matches.select('name').distinct().pl())}"
)

exact_matches_collected = exact_matches.pl()

# For now return only the exact matches

exact_matches_cleaned = exact_matches_collected.select(
    "ceeb",
    "ipeds",
    "ceeb_name",
    "ipeds_name",
    "edition",
    "multicampus",
    "city",
    "county_name",
    "zip",
    "state",
    "state_abbr",
    "state_fips",
    "fips",
    "latitude",
    "longitude",
).sort("ceeb")

duck.execute(
    """
    CREATE OR REPLACE TABLE university_crosswalk AS (
        select 
            a.ceeb,
            a.ipeds,
            b.nsc,
            a.ceeb_name,
            a.ipeds_name,
            nsc_name: b.name,
            ipeds_edition: a.edition,
            a.city,
            a.county_name,
            a.zip,
            a.state,
            a.state_abbr,
            a.state_fips,
            a.fips,
            a.latitude,
            a.longitude
        from exact_matches_cleaned a
        left join nsc_university b using (ipeds)
        order by ipeds
    )
    """
)

