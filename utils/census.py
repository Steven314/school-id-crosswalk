import os
import requests
import zipfile
import time
import duckdb
from utils.duckdb import table_exists


class Census:
    def __init__(
        self,
        table_name: str,
        source_name: str,
        year: str | int,
        state: str | int = "us",
    ):
        # parameters
        self.year = year
        self.state = f"{state:02}"
        self.table_name = table_name

        # build URL
        self.file = f"tl_{self.year}_{self.state}_{source_name.lower()}.zip"

        self.base_name = self.file[:-4]

        self.base_url = (
            "https://www2.census.gov/geo/tiger"
            f"/TIGER{year}/{source_name.upper()}/"
        )
        self.url = self.base_url + self.file

        # storage locations
        self.extracted_location = os.path.join(
            "extracted-zips", self.base_name
        )

        self.raw_file_loc = os.path.join("raw-data", self.file)

        self.shape_file = os.path.join(
            self.extracted_location, self.base_name + ".shp"
        )

    def download(self) -> None:
        """Conditionally download raw data from a URL."""

        if not os.path.exists(self.raw_file_loc):
            resp = requests.get(self.url)

            with open(self.raw_file_loc, mode="wb") as file:
                file.write(resp.content)

            # sleep to avoid being rate limited
            time.sleep(1)

        return None

    def extract(self) -> None:
        """Conditionally extract the contents of the ZIP file."""

        if not os.path.exists(self.raw_file_loc):
            raise FileNotFoundError("The ZIP file was not found.")

        if not os.path.exists(self.extracted_location):
            with zipfile.ZipFile(self.shape_file, "r") as z:
                z.extractall(self.extracted_location)

        return None

    def append_to_duckdb(self, duck_con: duckdb.DuckDBPyConnection) -> None:
        """Append the Data to DuckDB

        Args:
            duck_con (duckdb.DuckDBPyConnection): The DuckDB connection.

        Raises:
            FileNotFoundError: When the shapefile is not found.

        Returns:
            None: Returns nothing.
        """

        sql = (
            f"select *, edition: {self.year}::INT "
            + f"from st_read('{self.shape_file}')"
        )

        if not os.path.exists(self.shape_file):
            raise FileNotFoundError("The shapefile was not found.")

        # if the table does not exist at all, add this segment to the table.
        if not table_exists(duck_con, self.table_name):
            duck_con.sql(sql).create(self.table_name)
        else:
            query_str = f"edition = {self.year}"
            if self.state != "us":
                query_str += f" and statefp = '{self.state}'"

            # is the current state/year combination already present?
            current_rows_present = (
                duck_con.table(self.table_name).filter(query_str).shape[0]
            )

            # if not present, insert it.
            if current_rows_present == 0:
                duck_con.execute(f"INSERT INTO {self.table_name} " + sql)

        return None
