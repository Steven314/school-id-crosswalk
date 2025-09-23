import duckdb
from utils.duckdb import install_and_load, table_exists
from utils.census import census_download, census_extract
import os


class SchoolData:
    def __init__(self, year: str | int, state: str | int):
        self.year = year
        self.state = f"{state:02}"

        self.file = f"tl_{self.year}_{self.state}_unsd.zip"
        self.base_name = self.file[:-4]

        self.base_url = f"https://www2.census.gov/geo/tiger/TIGER{year}/UNSD/"
        self.url = self.base_url + self.file

    def download(self):
        census_download(url=self.url, dest_file=self.file)

        self.raw_file_loc = os.path.join("raw-data", self.file)

    def unzip(self):
        self.extracted_location = os.path.join(
            "extracted-zips", self.base_name
        )

        census_extract(self.raw_file_loc, self.extracted_location)

        self.shape_file = os.path.join(
            self.extracted_location, self.base_name + ".shp"
        )

    def read(self, duck_con: duckdb.DuckDBPyConnection):
        if not os.path.exists(self.shape_file):
            raise FileNotFoundError("The shapefile was not found.")

        return duck_con.sql(
            f"select *, edition: {self.year}::INT from st_read('{self.shape_file}')"
        )

    def append_to_duckdb(
        self,
        duck_con: duckdb.DuckDBPyConnection,
    ):
        sql = f"select *, edition: {self.year}::INT from st_read('{self.shape_file}')"

        if not os.path.exists(self.shape_file):
            raise FileNotFoundError("The shapefile was not found.")

        # if the table does not exist at all, add this segment to the table.
        if not table_exists(duck_con, "school_districts"):
            duck_con.sql(sql).create("school_districts")
        else:
            # is the current state already present?
            current_rows_present = (
                duck_con.table("school_districts")
                .filter(f"statefp = '{self.state}'")
                .shape[0]
            )

            if current_rows_present == 0:
                duck_con.execute("INSERT INTO school_districts " + sql)


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

    for x in states:
        x.download()
        x.unzip()

    with duckdb.connect(  # type: ignore
        "clean-data/geography.duckdb"
    ) as duck:
        duckdb.DuckDBPyConnection

        install_and_load(duck, "spatial", True)

        for x in states:
            x.append_to_duckdb(duck)
