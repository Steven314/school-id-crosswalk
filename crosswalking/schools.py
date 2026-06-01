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

        self.crosswalk_sql_dir = crosswalk_sql_dir

        # the required SQL files
        self.sql_file_names = [
            "ceeb_school",
            "nces_school",
            "college_board_matches",
        ]
        self.sql_files = [
            os.path.join(self.crosswalk_sql_dir, file + ".sql")
            for file in self.sql_file_names
        ]

        # the tables to be created.
        self.table_names = ["ceeb", "nces", "cb_match"]

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
            self.duck.duck.create_function("tighten", tighten)  # type: ignore

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

        # first, create the non-matched CEEB and NCES records based on the
        # College Board's matches.
        self.duck.create_table_query(
            table_name="cb_unmatched_ceeb",
            query="from ceeb anti join cb_match using (ceeb)",
        )

        self.duck.create_table_query(
            table_name="cb_unmatched_nces",
            query="from nces anti join cb_match using (nces)",
        )

        round_1 = iterate(
            "1",
            None,
            "cb_unmatched_ceeb",
            "cb_unmatched_nces",
            ["name", "address", "city", "state_abbr", "zip"],
            ["address", "city", "state_abbr", "zip"],
        )

        round_2 = iterate(
            "2",
            ["name"],
            "non_1_ceeb",
            "non_1_nces",
            ["address", "city", "state_abbr", "zip"],
            ["address", "city", "state_abbr", "zip"],
        )

        round_3 = iterate(
            "3",
            ["address"],
            "non_2_ceeb",
            "non_2_nces",
            ["name", "city", "state_abbr", "zip"],
            ["name", "city", "state_abbr", "zip"],
        )

        round_4 = iterate(
            "4",
            ["address", "zip"],
            "non_3_ceeb",
            "non_3_nces",
            ["name", "city", "state_abbr"],
            ["name", "city", "state_abbr"],
        )

        round_5 = iterate(
            "5",
            ["address", "city"],
            "non_4_ceeb",
            "non_4_nces",
            ["name", "state_abbr", "zip"],
            ["name", "state_abbr", "zip"],
        )

        round_6 = iterate(
            "6",
            ["address", "city", "zip"],
            "non_5_ceeb",
            "non_5_nces",
            ["name", "state_abbr"],
            ["name", "state_abbr"],
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
            "state_abbr",
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

    def build_crosswalk(self):
        self.duck.create_table_file(
            "crosswalk",
            os.path.join(self.crosswalk_sql_dir, "school_crosswalk.sql"),
        )

    def save_crosswalk(
        self,
        polars: bool = False,
        csv: bool = True,
        csv_file: str = "crosswalk.csv",
    ) -> pl.DataFrame | None:
        data = self.duck.table("crosswalk").pl()

        if csv:
            with open(csv_file, "w", encoding="utf8") as f:
                data.write_csv(f)

        if polars:
            return data
        else:
            return None

    def potential_duplicate_matching(self):
        def unmatched(tbl: str):
            return f"from schools.{tbl} anti join unique_exact_matches using ({tbl})"

        unmatched_ceeb = unmatched("ceeb")
        unmatched_nces = unmatched("nces")

        name_zip = (
            "select\n"
            "    * exclude (a.state, a.state_abbr, a.address, a.city),\n"
            "    strength: '(potential duplicates) name, zip'\n"
            "from unmatched_ceeb a\n"
            "inner join unmatched_nces b using (name, zip)\n"
        )

        unmatched_ceeb_2 = (
            "from unmatched_ceeb anti join name_zip using (ceeb)"
        )

        name_state = (
            "select\n"
            "    * exclude (a.zip, a.city, a.address),\n"
            "    strength: '(potential duplicates) name, state'\n"
            "from unmatched_ceeb_2 a\n"
            "inner join unmatched_nces b using (name, state, state_abbr)\n"
        )

        combined = (
            "select * exclude (public_private, edition)\n"
            "from (\n"
            # "    (from unique_exact_matches) union all by name \n"
            "    (from name_zip) union all by name \n"
            "    (from name_state)\n"
            ")\n"
        )

        sql = (
            "with\n"
            f"  unmatched_ceeb as ({unmatched_ceeb}),\n"
            f"  unmatched_nces as ({unmatched_nces}),\n"
            f"  name_zip as ({name_zip}),\n"
            f"  unmatched_ceeb_2 as ({unmatched_ceeb_2}),\n"
            f"  name_state as ({name_state}),\n"
            f"  combined as ({combined})\n"
            "select * from combined"
        )

        self.duck.create_table_query("potential_duplicate_matches", sql)

        return self.duck.sql("from potential_duplicate_matches").pl()

    def collect_exact_matches(self):
        """Collect the Exact Matches"""

        self.duck.create_table_query(
            "exact_matches",
            "from unique_exact_matches union all by name (from potential_duplicate_matches)",
        )

        self.duck.create_table_query(
            "unmatched_nces",
            "from schools.nces anti join exact_matches using (nces)",
        )

        self.duck.create_table_query(
            "unmatched_ceeb",
            "from schools.ceeb anti join exact_matches using (ceeb)",
        )

    def find_exact_matches(self):
        """Finds exact matches in two stages.

        1. Exact matches by name and ZIP code.
        2. Exact matches by name and state.

        The SQL is a bit complex, but the process is to create tables based on
        the exact matches and non-matched records for each step. The nested
        functions are just to short-hand building the SQL.

        This creates tables instead of views because the DuckDB FTS extension
        can only access tables, not views.
        """

        # these columns are used repeatedly.
        def cols(method: str):
            names = [
                f"method: '{method}'",
                "name",
                "ceeb",
                "nces",
                "ceeb_name",
                "nces_name",
                "ceeb_address: a.address",
                "nces_address: b.address",
                "ceeb_city: a.city",
                "nces_city: b.city",
                "nces_county_name: county_name",
                "ceeb_zip: a.zip",
                "nces_zip: b.zip",
                "ceeb_state: a.state",
                "nces_state: b.state",
                "ceeb_state_abbr: a.state_abbr",
                "nces_state_abbr: b.state_abbr",
                "state_fips",
                "fips",
                "latitude",
                "longitude",
                "public_private",
            ]

            return ", ".join(names)

        def distinct(val: str, by: str):
            return f"distinct_{val}: count(distinct {val}) over (partition by {by})"

        def enforce_one_to_one(query: str):
            dist_a = distinct("nces", "ceeb")
            dist_b = distinct("ceeb", "nces")

            n = f"select {dist_a}, {dist_b}, * from matches"

            return (
                f"with matches as ({query}), n as ({n})"
                "select * exclude(distinct_nces, distinct_ceeb) "
                "from n where distinct_nces = 1 and distinct_ceeb = 1"
            )

        # round 1, exact by name and zip, one-to-one relationship
        exact_name_zip = enforce_one_to_one(
            f"select {cols('exact name+zip')} "
            "from ceeb a "
            "inner join nces b using (name, zip) "
            "order by ceeb"
        )

        self.duck.create_table_query("exact_name_zip", exact_name_zip)

        # get non-matching from round 1
        r1_non_exact_ceeb = "from ceeb anti join nces using (name, zip)"
        r1_non_exact_nces = "from nces anti join ceeb using (name, zip)"

        self.duck.create_table_query("r1_non_exact_ceeb", r1_non_exact_ceeb)
        self.duck.create_table_query("r1_non_exact_nces", r1_non_exact_nces)

        # round 2
        # matches with name and state, one-to-one relationship
        exact_name_state = enforce_one_to_one(
            f"select {cols('exact name+state')} "
            "from r1_non_exact_ceeb a "
            "inner join r1_non_exact_nces b using (name, state) "
            "order by ceeb"
        )

        self.duck.create_table_query("exact_name_state", exact_name_state)

        # remaining unmatched records
        r2_non_exact_ceeb = (
            "from ceeb "
            "anti join ("
            "from exact_name_state union all by name from exact_name_zip"
            ") using (ceeb)"
        )
        r2_non_exact_nces = (
            "from nces "
            "anti join ("
            "from exact_name_state union all by name from exact_name_zip"
            ") using (nces)"
        )

        self.duck.create_table_query("r2_non_exact_ceeb", r2_non_exact_ceeb)
        self.duck.create_table_query("r2_non_exact_nces", r2_non_exact_nces)


if __name__ == "__main__":
    with DuckDB(os.path.join("crosswalking", "schools.duckdb")) as duck:

        school = SchoolCrosswalk(
            duck=duck,
            clean_data_dir="clean-data",
            crosswalk_sql_dir=os.path.join("crosswalking", "sql"),
        )

        school.attach_dbs()

        school.create_school_tables()
        school.iterative_exact_matching()
        school.build_crosswalk()
        school.save_crosswalk(csv_file="crosswalking\\school_crosswalk.csv")
