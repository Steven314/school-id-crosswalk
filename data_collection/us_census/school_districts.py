import duckdb
from utils.duckdb import install_and_load
from utils.census import Census


class SchoolData(Census):
    def __init__(self, year: str | int, state: str | int):
        super().__init__(
            year=year,
            state=state,
            table_name="school_district",
            source_name="unsd",
        )


if __name__ == "__main__":
    import ftplib
    import re

    year = 2024

    # probe the FTP server for all of the state school district files.
    with ftplib.FTP("ftp2.census.gov") as ftp:
        ftp.login(user="anonymous")

        ftp.cwd(f"geo/tiger/TIGER{year}/UNSD")

        files = ftp.nlst()

    # extract the FIPS values.
    state_fips = [
        re.search(r"tl_\d{4}_(\d{2})_unsd.zip", f).group(1)  # type: ignore
        for f in files
    ]

    states = [SchoolData(year, fips) for fips in state_fips]

    with duckdb.connect(  # type: ignore
        "clean-data/geography.duckdb"
    ) as duck:
        duckdb.DuckDBPyConnection

        install_and_load(duck, "spatial", True)

        for x in states:
            print(x.state)

            x.download()
            x.extract()
            x.append_to_duckdb(duck)
