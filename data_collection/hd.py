import os
from typing import List

from utils.duckdb import DuckDB
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

    def append_to_duckdb(self, duck: DuckDB) -> None:
        """Append the Data to DuckDB

        Args:
            duck (DuckDB): A DuckDB object.
        """

        # The some of the columns have extra spaces which needlessly increases
        # the file size. We need to trim only the VARCHAR columns.
        #
        # Source: https://github.com/duckdb/duckdb/discussions/10842
        #
        # Also, " " needs to be considered NULL.

        sql = (
            "select if("
            "typeof(columns(*)) = 'VARCHAR', "
            "cast_to_type(trim(cast_to_type(columns(*), '')), columns(*)), "
            "columns(*)"
            "), "
            "edition: regexp_extract(filename, '\\d{4}')::INT "
            "from read_csv("
            f"'{os.path.join(self.extraction_location, '**.csv')}',"
            "ignore_errors = true, union_by_name = true, nullstr = ' '"
            ")"
        )

        if not os.path.exists(self.extraction_location):
            raise FileNotFoundError("The folder of CSVs was not found.")

        duck.create_table_query(self.combined_table_name, sql)

        return None


if __name__ == "__main__":
    years = list(range(2009, 2024 + 1))

    hd = HD(years)

    hd.download()
    hd.extract()

    with DuckDB("clean-data/ipeds.duckdb") as duck:
        hd.append_to_duckdb(duck)
