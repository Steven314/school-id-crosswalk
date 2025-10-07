from utils.census import Census
from utils.duckdb import DuckDB


class ZIPCodeData(Census):
    def __init__(self, year: str | int):
        super().__init__(
            year=year,
            table_name="zcta",
            source_name="zcta520",
        )


if __name__ == "__main__":
    year = 2025

    zips = ZIPCodeData(year)

    with DuckDB("clean-data/geography.duckdb") as duck:
        duck.install_and_load_extension("spatial", True)

        zips.download()
        zips.extract()
        zips.append_to_duckdb(duck)
