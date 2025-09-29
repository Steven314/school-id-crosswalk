import duckdb

from utils.census import Census
from utils.duckdb import install_and_load


class StateData(Census):
    def __init__(self, year: str | int):
        super().__init__(
            year=year,
            table_name="state",
            source_name="state",
        )


if __name__ == "__main__":
    year = 2025

    state = StateData(year)

    with duckdb.connect(  # type: ignore
        "clean-data/geography.duckdb"
    ) as duck:
        duckdb.DuckDBPyConnection

        install_and_load(duck, "spatial", True)

        state.download()
        state.extract()
        state.append_to_duckdb(duck)
