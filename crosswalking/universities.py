import os
from typing import Any, List

import duckdb
import Levenshtein
import polars as pl
from thefuzz import fuzz  # type: ignore

from utils.duckdb import (
    attach_db,
    create_fts_index,
    create_table_file,
    create_table_query,
)


class UniversityCrosswalk:
    def __init__(
        self,
        duck: duckdb.DuckDBPyConnection,
        clean_data_dir: str,
        crosswalk_sql_dir: str,
    ):
        self.duck = duck

        # the required SQL files
        self.sql_file_names = ["hd", "ceeb_university", "nsc"]
        self.sql_files = [
            os.path.join(crosswalk_sql_dir, file + ".sql")
            for file in self.sql_file_names
        ]

        # the tables to be created.
        self.table_names = ["ipeds_hd", "ceeb_university", "nsc_university"]

        # the required DuckDB files.
        self.db_names = ["ceeb", "ipeds", "geography", "nsc"]

        self.db_paths = [
            os.path.join(clean_data_dir, db + ".duckdb")
            for db in self.db_names
        ]

    def process(self):
        print("Initialization...")
        self.attach_dbs()
        self.create_university_tables()

        print("Finding Exact Matches...")
        self.find_exact_matches()

        print(f"Exact Match Quality: {self.exact_match_quality():.2%}")

        print("Filtering Non-Exact Matches...")
        self.find_non_exact_ceeb()
        self.create_non_exact_multicampus()

        print("Searching Index...")
        self.build_index()
        self.iterate_searching()
        self.fuzzy_distance()

        print("Writing Crosswalk")
        self.write_crosswalk()

    def attach_dbs(self):
        for path in self.db_paths:
            attach_db(self.duck, path)

    def create_university_tables(self):
        for file, name in zip(self.sql_files, self.table_names):
            create_table_file(self.duck, name, file)

    def build_index(self):
        create_fts_index(
            self.duck,
            input_table="non_exact_multicampus",
            input_id="ipeds",
            input_values=["name", "city"],
            stemmer="none",
            stopwords="none",
            overwrite=1,
        )

    def search_index(
        self,
        search_string: str,
        search_state: str,
        search_ceeb: str,
        limit: int = 5,
    ) -> pl.DataFrame:
        sql = (
            "SELECT * "
            "FROM ("
            "    SELECT "
            "        *, "
            "        fts_main_non_exact_multicampus.match_bm25("
            "            ipeds, "
            "            $search_string"
            "        ) AS match_score "
            # "    FROM ipeds_hd"
            "    FROM non_exact_multicampus"
            ")"
            "WHERE match_score IS NOT NULL "
            "AND lower(state) = lower($search_state)"
            "ORDER BY match_score DESC "
            "LIMIT $limit"
        )

        params: dict[str, str | int] = {
            "search_string": search_string,
            "search_state": search_state,
            "limit": limit,
        }

        search_results = (
            self.duck.sql(sql, params=params)
            .pl()
            .with_columns(
                search_name=pl.lit(search_string),
                search_state=pl.lit(search_state),
                ceeb=pl.lit(search_ceeb),
            )
        )

        return search_results

    def iterate_searching(self):
        result: List[pl.DataFrame] = []

        for row in self.realize_non_exact_ceeb().iter_rows(named=True):
            result.append(
                self.search_index(
                    search_string=row["name"],
                    search_state=row["state"],
                    search_ceeb=row["ceeb"],
                    limit=10,
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

        self.bm25_matches = (
            pl.concat(result).sort("match_score", descending=True).sort("name")
        )

    def find_exact_matches(self):
        self.exact_matches = self.duck.sql(
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
                a.state,
                state_abbr,
                state_fips,
                fips,
                latitude,
                longitude 
            from ipeds_hd a 
            inner join ceeb_university b 
            on (lower(a.name) = lower(b.name) and a.state = b.state)
            """
        )

    def realize_exact_matches(self):
        if not hasattr(self, "exact_matches_pl"):
            self.exact_matches_pl = self.exact_matches.pl()

        return self.exact_matches_pl

    def exact_match_quality(self):
        n_ipeds = len(self.duck.sql("from ipeds_hd").pl())
        n_ceebs = len(self.duck.sql("from ceeb_university").pl())
        n_exact = len(self.exact_matches.pl())

        coverage = n_exact / min(n_ipeds, n_ceebs)

        return coverage

    def find_non_exact_ceeb(self):
        self.non_exact_ceeb = self.duck.table("ceeb_university").join(
            self.exact_matches.filter("multicampus or multicampus is null"),
            condition="ceeb",
            how="anti",
        )

    def realize_non_exact_ceeb(self) -> pl.DataFrame:
        if not hasattr(self, "non_exact_ceeb_pl"):
            self.non_exact_ceeb_pl = self.non_exact_ceeb.pl()

        return self.non_exact_ceeb_pl

    def create_non_exact_multicampus(self):
        query = (
            self.duck.table("ipeds_hd")
            .join(
                self.exact_matches.filter(
                    "multicampus or multicampus is null"
                ),
                condition="ipeds",
                how="anti",
            )
            .sql_query()
        )

        create_table_query(self.duck, "non_exact_multicampus", query)

    def levenshtein_ratio(self, row: dict[str, Any]) -> float:
        """
        Levenshtein ratio by searching over the four combinations of the two
        names and appending or not appending the city, taking the maximum
        ratio.
        """

        name_name = Levenshtein.ratio(row["search_name"], row["name"])
        name_city = Levenshtein.ratio(row["search_name"], row["name_city"])
        city_name = Levenshtein.ratio(row["search_name_city"], row["name"])
        city_city = Levenshtein.ratio(
            row["search_name_city"], row["name_city"]
        )

        return max(name_name, name_city, city_name, city_city)

    def fuzz_ratio(self, row: dict[str, Any]) -> float:
        """
        Fuzz ratio by searching over the four combinations of the two
        names and appending or not appending the city, taking the maximum
        ratio.
        """

        name_name = fuzz.token_sort_ratio(  # type: ignore
            row["search_name"], row["name"]
        )
        name_city = fuzz.token_sort_ratio(  # type: ignore
            row["search_name"], row["name_city"]
        )
        city_name = fuzz.token_sort_ratio(  # type: ignore
            row["search_name_city"], row["name"]
        )
        city_city = fuzz.token_sort_ratio(  # type: ignore
            row["search_name_city"], row["name_city"]
        )

        return max(name_name, name_city, city_name, city_city) / 100

    def fuzzy_distance(self):
        fuzzy_matches = (  # noqa: F841 # type: ignore
            self.bm25_matches.with_columns(
                search_name_city=pl.col("search_name") + " " + pl.col("city"),
                name_city=pl.col("name") + " " + pl.col("city"),
            )
            .with_columns(
                similarity_ratio=pl.struct(pl.all()).map_elements(
                    self.levenshtein_ratio,
                    return_dtype=pl.Float64,
                ),
                fuzz_ratio=pl.struct(pl.all()).map_elements(
                    self.fuzz_ratio, return_dtype=pl.Float64
                ),
            )
            .sort("similarity_ratio")
            .drop("search_name_city", "name_city")
            .filter(
                (
                    (pl.col("match_score") > 8)
                    | (pl.col("similarity_ratio") > 0.9)
                    | (pl.col("fuzz_ratio") > 0.95)
                )
                & (pl.col("match_score") > 4)
            )
            .sort(
                by=["ipeds", "match_score", "similarity_ratio"],
                descending=[False, True, True],
            )
            .group_by("ipeds")
            .first()
        )

        duck.execute(
            "create or replace table fuzzy_matches as (from fuzzy_matches)"
        )

        self.fuzzy_matches = self.duck.table("fuzzy_matches")

    def write_crosswalk(self):
        def cols(const: str):
            return [
                duckdb.ColumnExpression("ceeb"),
                duckdb.ColumnExpression("ipeds"),
                duckdb.ColumnExpression("state"),
                duckdb.ConstantExpression(const).alias("method"),
            ]

        # these are all used implicitly
        exact = self.exact_matches.select(*cols("exact"))  # type: ignore  # noqa: F841
        fuzzy = self.fuzzy_matches.select(*cols("fuzzy"))  # type: ignore  # noqa: F841

        ceeb = self.duck.table("ceeb_university")  # type: ignore  # noqa: F841
        hd = self.duck.table("ipeds_hd")  # type: ignore  # noqa: F841
        nsc = self.duck.table("nsc_university")  # type: ignore  # noqa: F841

        crosswalk = self.duck.sql(
            """
            SELECT 
                method: COALESCE(method, 'no-ceeb-match'),
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
            FROM ((FROM fuzzy) UNION ALL BY NAME (FROM exact))
            FULL JOIN ceeb USING (ceeb, state)
            FULL JOIN hd h USING (ipeds, state)
            FULL JOIN nsc n USING (ipeds)
            """
        )

        create_table_query(
            duck=self.duck,
            table_name="university_crosswalk",
            query=crosswalk.sql_query(),
        )


if __name__ == "__main__":
    with duckdb.connect(  # type: ignore
        os.path.join("crosswalking", "universities.duckdb")
    ) as duck:
        duckdb.DuckDBPyConnection

        univ = UniversityCrosswalk(
            duck=duck,
            clean_data_dir="clean-data",
            crosswalk_sql_dir=os.path.join("crosswalking", "sql"),
        )

        univ.process()
