import duckdb

from utils.census import Census
from utils.duckdb import install_and_load


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

    with duckdb.connect(  # type: ignore
        "clean-data/geography.duckdb"
    ) as duck:
        duckdb.DuckDBPyConnection

        install_and_load(duck, "spatial", True)

        zips.download()
        zips.extract()
        zips.append_to_duckdb(duck)
