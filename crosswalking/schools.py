import os
import re
from typing import List

import polars as pl

from utils.duckdb import DuckDB


class SchoolCrosswalk:
    def __init__(
        self,
        duck: DuckDB,
        clean_data_dir: str,
        crosswalk_sql_dir: str,
    ):
        self.duck = duck

        # the required SQL files
        self.sql_file_names = ["ceeb_school", "nces_school"]
        self.sql_files = [
            os.path.join(crosswalk_sql_dir, file + ".sql")
            for file in self.sql_file_names
        ]

        # the tables to be created.
        self.table_names = ["ceeb", "nces"]

        # the required DuckDB files.
        self.db_names = ["ceeb", "geography", "nces"]

        self.db_paths = [
            os.path.join(clean_data_dir, db + ".duckdb")
            for db in self.db_names
        ]

    def attach_dbs(self):
        for path in self.db_paths:
            self.duck.attach_db(path)

    def create_school_tables(self):
        def tighten(string: str) -> str:
            return re.sub(
                r"((?:\b\w\b\s*){2,})(?=\s|$)",
                lambda m: m.group(1).replace(" ", ""),
                string,
            )

        if (
            len(
                self.duck.sql(
                    "from duckdb_functions() where function_name = 'tighten'"
                ).pl()
            )
            == 0
        ):
            self.duck.duck.create_function("tighten", tighten)

        for file, name in zip(self.sql_files, self.table_names):
            self.duck.create_table_file(name, file)

    def iterative_exact_matching(self, return_sql: bool = False):
        """
        This builds out a large SQL query for exact matching with no duplicates.
        """

        def non_matching(
            id: str, ceeb_nces: str, table: str, partition_by: List[str]
        ):
            return (
                f"non_{id}_{ceeb_nces} as (\n"
                f"    SELECT * \n"
                f"    FROM {table}\n"
                f"    ANTI JOIN exact_{id}\n"
                f"    USING ({ceeb_nces})\n"
                ")"
            )

        def iterate(
            id: str,
            excluding: List[str] | None,
            ceeb_table: str,
            nces_table: str,
            using: List[str],
            partition_by: List[str],
        ):
            if excluding is None:
                exclude = ""
            else:
                excluding_expanded = [
                    t + "." + e
                    for t, e in zip([ceeb_table] * len(excluding), excluding)
                ]

                exclude = f"EXCLUDE ({', '.join(excluding_expanded)})"

            strength = f"strength: '{id}) {', '.join(using)}'"

            exact = (
                f"exact_{id} AS (\n"
                f"    SELECT * {exclude}, {strength}\n"
                f"    FROM {ceeb_table}\n"
                f"    INNER JOIN {nces_table}\n USING ({', '.join(using)})\n"
                f"    QUALIFY count(*) OVER (PARTITION BY {', '.join(partition_by)}) = 1\n"
                ")"
            )

            non_ceeb = non_matching(id, "ceeb", ceeb_table, partition_by)
            non_nces = non_matching(id, "nces", nces_table, partition_by)

            sql = ",\n".join([exact, non_ceeb, non_nces])

            return sql

        round_1 = iterate(
            "1",
            None,
            "(select * exclude nces from schools.ceeb where nces is null)",
            "(from schools.nces anti join schools.ceeb using (nces))",
            ["name", "address", "city", "state", "state_abbr", "zip"],
            ["address", "city", "state", "state_abbr", "zip"],
        )

        round_2 = iterate(
            "2",
            ["name"],
            "non_1_ceeb",
            "non_1_nces",
            ["address", "city", "state", "state_abbr", "zip"],
            ["address", "city", "state", "state_abbr", "zip"],
        )

        round_3 = iterate(
            "3",
            ["address"],
            "non_2_ceeb",
            "non_2_nces",
            ["name", "city", "state", "state_abbr", "zip"],
            ["name", "city", "state", "state_abbr", "zip"],
        )

        round_4 = iterate(
            "4",
            ["address", "zip"],
            "non_3_ceeb",
            "non_3_nces",
            ["name", "city", "state", "state_abbr"],
            ["name", "city", "state", "state_abbr"],
        )

        round_5 = iterate(
            "5",
            ["address", "city"],
            "non_4_ceeb",
            "non_4_nces",
            ["name", "state", "state_abbr", "zip"],
            ["name", "state", "state_abbr", "zip"],
        )

        round_6 = iterate(
            "6",
            ["address", "city", "zip"],
            "non_5_ceeb",
            "non_5_nces",
            ["name", "state", "state_abbr"],
            ["name", "state", "state_abbr"],
        )

        rounds = [round_1, round_2, round_3, round_4, round_5, round_6]

        union = " union all by name\n    ".join(
            [f"from exact_{i + 1}" for i in range(len(rounds))]
        )

        selection = [
            "strength",
            "ceeb",
            "nces",
            "name",
            "ceeb_name",
            "nces_name",
            "address",
            "city",
            "state",
            "state_abbr",
            "state_fips",
            "county_name",
            "fips",
            "zip",
            "latitude",
            "longitude",
        ]

        sql = (
            f"WITH {', '.join(rounds)}\n"
            f"SELECT {', '.join(selection)}\n"
            f"FROM (\n    {union}\n)"
        )

        if return_sql:
            return sql
        else:
            self.duck.create_table_query("unique_exact_matches", sql)

            return self.duck.sql("from unique_exact_matches").pl()

duck = DuckDB(os.path.join("crosswalking", "schools.duckdb"))


school = SchoolCrosswalk(
    duck=duck,
    clean_data_dir="clean-data",
    crosswalk_sql_dir=os.path.join("crosswalking", "sql"),
)

school.attach_dbs()

school.create_school_tables()
# school.find_exact_matches()


school.iterative_exact_matching()
