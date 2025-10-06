import os
from typing import Any, List

import duckdb
import Levenshtein
import polars as pl
from thefuzz import fuzz  # type: ignore


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


def search_index(
    duck: duckdb.DuckDBPyConnection,
    name: str,
    state: str,
    ceeb: str,
    limit: int = 5,
):
    sql = (
        "SELECT * "
        "FROM ("
        "    SELECT *, fts_main_ipeds_multi_non_exact.match_bm25("
        "        ipeds, "
        "        ?"
        "    ) AS match_score "
        "    FROM ipeds_hd"
        ")"
        "WHERE match_score IS NOT NULL "
        "AND lower(state) = lower(?)"
        "ORDER BY match_score DESC "
        "LIMIT ?"
    )

    search_results = (
        duck.sql(sql, params=[name, state, limit])
        .pl()
        .with_columns(
            search_name=pl.lit(name),
            search_state=pl.lit(state),
            ceeb=pl.lit(ceeb),
        )
    )

    return search_results


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

exact_matches_cleaned = duck.sql(
    """
    select 
        method: 'exact',
        ceeb,
        ipeds,
        ceeb_name,
        ipeds_name,
        edition,
        multicampus,
        city,
        county_name,
        zip,
        state,
        state_abbr,
        state_fips,
        fips,
        latitude,
        longitude
    from exact_matches
    order by ceeb
    """
)


# 6830 rows
ipeds_hd = duck.sql("from ipeds_hd")
non_exact_ipeds = ipeds_hd.join(
    exact_matches,
    condition=f"lower({exact_matches.alias}.name) = lower({ipeds_hd.alias}.name) and {exact_matches.alias}.state = {ipeds_hd.alias}.state",
    how="anti",
)
print(len(non_exact_ipeds))

# 979 rows
ceeb_university = duck.sql("from ceeb_university")
non_exact_ceeb = ceeb_university.join(
    exact_matches,
    condition=f"lower({exact_matches.alias}.name) = lower({ceeb_university.alias}.name) and {exact_matches.alias}.state = {ceeb_university.alias}.state",
    how="anti",
)
print(len(non_exact_ceeb))

duck.execute(
    f"create or replace table non_exact_ipeds as ({non_exact_ipeds.sql_query()})"
)
duck.execute(
    f"create or replace table non_exact_ceeb as ({non_exact_ceeb.sql_query()})"
)


# exact matches that shouldn't have branch campuses.
exact_no_multi = exact_matches_cleaned.pl().filter(~pl.col("multicampus"))
exact_multi = exact_matches_cleaned.pl().filter(
    pl.col("multicampus") | pl.col("multicampus").is_null()
)

# potential multicampus IPEDS codes.
duck.execute(
    "create or replace table ipeds_multi_non_exact as (from ipeds_hd anti join exact_no_multi using (ipeds))"
)

# create the index
duck.execute(
    "PRAGMA create_fts_index("
    # "    non_exact_ipeds, "
    # "    ipeds_hd, "
    "    ipeds_multi_non_exact, "
    "    ipeds, "
    "    name, "
    "    city, "
    "    stemmer = 'none', "
    "    stopwords = 'none', "
    "    overwrite = 1"
    ")"
)


# match against it
non_exact_ceeb_collected = non_exact_ceeb.pl()

result: List[pl.DataFrame] = []

for row in non_exact_ceeb_collected.iter_rows(named=True):
    print(row)

    result.append(
        search_index(
            duck, row["name"], row["state"], row["ceeb"], limit=10
        ).select(
            "match_score",
            "ceeb",
            "search_name",
            "ipeds",
            "name",
            "city",
            "state",
            "edition",
        )
    )

# inspect matches

result_df = pl.concat(result).sort("match_score", descending=True).sort("name")


def levenshtein_distance_names(row: dict[str, Any]) -> float:
    """
    Searches over the four combinations of the two names and appending or not
    appending the city.
    """
    name_name = Levenshtein.ratio(row["search_name"], row["name"])
    name_city = Levenshtein.ratio(row["search_name"], row["name_city"])
    city_name = Levenshtein.ratio(row["search_name_city"], row["name"])
    city_city = Levenshtein.ratio(row["search_name_city"], row["name_city"])

    return max(name_name, name_city, city_name, city_city)


def fuzz_ratio(row: dict[str, Any]) -> float:
    name_name = fuzz.token_sort_ratio(row["search_name"], row["name"])
    name_city = fuzz.token_sort_ratio(row["search_name"], row["name_city"])
    city_name = fuzz.token_sort_ratio(row["search_name_city"], row["name"])
    city_city = fuzz.token_sort_ratio(
        row["search_name_city"], row["name_city"]
    )

    return max(name_name, name_city, city_name, city_city) / 100


result_distance = (
    result_df.with_columns(
        search_name_city=pl.col("search_name") + " " + pl.col("city"),
        name_city=pl.col("name") + " " + pl.col("city"),
    )
    .with_columns(
        similarity_ratio=pl.struct(pl.all()).map_elements(
            levenshtein_distance_names, return_dtype=pl.Float64
        ),
        fuzz_ratio=pl.struct(pl.all()).map_elements(
            fuzz_ratio, return_dtype=pl.Float64
        ),
    )
    .sort("similarity_ratio")
).drop("search_name_city", "name_city")

fuzzy_matches = result_distance.filter(
    (
        (pl.col("match_score") > 8)
        | (pl.col("similarity_ratio") > 0.9)
        | (pl.col("fuzz_ratio") > 0.95)
    )
    & (pl.col("match_score") > 4)
).sort(
    by=["ipeds", "match_score", "similarity_ratio"],
    descending=[False, True, True],
)

best_fuzzy_match = fuzzy_matches.group_by("ipeds").first()

# Collecting the exact and fuzzy matches

# The idea is to narrow this down to the IDs and then join it with the full
# IPEDS, CEEB, and NSC tables.

duck.execute(
    """
    CREATE OR REPLACE TABLE university_crosswalk AS (
        SELECT 
            method,
            ceeb,
            ipeds,
            nsc,
            nsc_full,
            ceeb_name,
            ipeds_name,
            nsc_name: n.name,
            h.edition,
            multicampus,
            h.city,
            county_name, 
            zip,
            state,
            state_abbr,
            state_fips,
            fips,
            latitude,
            longitude
        FROM (
            (
                SELECT 
                    method: 'fuzzy',
                    ceeb, 
                    ipeds, 
                    state
                FROM best_fuzzy_match
            )
            UNION ALL BY NAME 
            (
                SELECT 
                    method: 'exact',
                    ceeb, 
                    ipeds, 
                    state
                FROM exact_matches
            )
        )
        FULL JOIN ceeb_university USING (ceeb, state)
        FULL JOIN ipeds_hd h USING (ipeds, state)
        FULL JOIN nsc_university n USING (ipeds)
        ORDER BY ipeds
    )
    """
)


duck.close()
