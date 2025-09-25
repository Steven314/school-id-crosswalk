import duckdb
from utils.duckdb import install_and_load
from utils.census import Census


class CountyData(Census):
    def __init__(self, year: str | int):
        super().__init__(
            year=year,
            table_name="county",
            source_name="county",
        )


if __name__ == "__main__":
    # With the counties there were some recent changes which affect some of the
    # older educational data. These are in Connecticut and Alaska. It may be
    # desirable to pull the 2010 data and the present data.

    year = 2025

    county = CountyData(year)

    with duckdb.connect(  # type: ignore
        "clean-data/geography.duckdb"
    ) as duck:
        duckdb.DuckDBPyConnection

        install_and_load(duck, "spatial", True)

        county.download()
        county.extract()
        county.append_to_duckdb(duck)
