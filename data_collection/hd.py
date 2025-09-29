import os
from typing import List

import duckdb

from utils.ipeds import IPEDS


class HD:
    def __init__(self, years: List[str] | List[int]):
        self.table_name = "hd"
        self.table_suffix = ""
        self.combined_table_name = self.table_name + self.table_suffix

        self.ipeds = [
            IPEDS(year, self.table_name, self.table_suffix) for year in years
        ]

        self.extraction_location = self.ipeds[1].extracted_location

    def download(self):
        for x in self.ipeds:
            x.download()

    def extract(self):
        for x in self.ipeds:
            x.extract()

    def append_to_duckdb(self, duck_con: duckdb.DuckDBPyConnection) -> None:
        """Append the Data to DuckDB

        Args:
            duck_con (duckdb.DuckDBPyConnection): The DuckDB connection.
        """

        sql = (
            "select *, edition: regexp_extract(filename, '\\d{4}')::INT "
            "from read_csv("
            f"'{os.path.join(self.extraction_location, '**.csv')}',"
            "ignore_errors = true, union_by_name = true"
            ")"
        )

        if not os.path.exists(self.extraction_location):
            raise FileNotFoundError("The folder of CSVs was not found.")

        duck_con.execute(
            f"CREATE OR REPLACE TABLE {self.combined_table_name} AS ({sql})"
        )

        return None


if __name__ == "__main__":
    years = list(range(2009, 2024 + 1))

    hd = HD(years)

    hd.download()
    hd.extract()

    with duckdb.connect(  # type: ignore
        "clean-data/ipeds.duckdb"
    ) as duck:
        duckdb.DuckDBPyConnection

        hd.append_to_duckdb(duck)
