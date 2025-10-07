import os

from utils.conditionals import conditional_download, conditional_extract
from utils.duckdb import DuckDB


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

    def download(self):
        conditional_download(self.url, self.raw_file_loc, sleep=True)

    def extract(self):
        conditional_extract(self.raw_file_loc, self.extracted_location)

    def append_to_duckdb(self, duck: DuckDB) -> None:
        """Append the Data to DuckDB

        Args:
            duck (DuckDB): A DuckDB object.
        """

        sql = (
            f"select *, edition: {self.year}::INT "
            + f"from st_read('{self.shape_file}')"
        )

        if not os.path.exists(self.shape_file):
            raise FileNotFoundError("The shapefile was not found.")

        # if the table does not exist at all, add this segment to the table.
        if not duck.table_exists(self.table_name):
            duck.sql(sql).create(self.table_name)
        else:
            query_str = f"edition = {self.year}"
            if self.state != "us":
                query_str += f" and statefp = '{self.state}'"

            # is the current state/year combination already present?
            current_rows_present = (
                duck.table(self.table_name).filter(query_str).shape[0]
            )

            # if not present, insert it.
            if current_rows_present == 0:
                duck.execute(f"INSERT INTO {self.table_name} " + sql)

        return None
