import duckdb
import os
from utils.duckdb import table_exists
from utils.conditionals import conditional_download, conditional_extract


class NCES:
    def __init__(self, year: str | int, school_type: str):
        """NCES

        Args:
            year (str | int): The year corresponding to the fall term.
                The datafiles are named '2324' for the 2023-2024 school
                year. For 2023-2024, use 2023. It will be internally
                transformed to '2324'.
            school_type (str): Public, private, or postsecondary.
        """

        self.school_type = self._convert_school_type(school_type)

        self.year = int(year)
        self.yr = self.year % 1000

        self.year_abb = str(self.yr) + str(self.yr + 1)

        self.file_name = f"EDGE_GEOCODE_{self.school_type}_{self.year_abb}.zip"
        self.base_name = self.file_name[:-4]

        self.base_url = "https://nces.ed.gov/programs/edge/data/"
        self.url = self.base_url + self.file_name

        self.raw_file_loc = os.path.join("raw-data", self.file_name)
        self.extracted_location = self._convert_extracted_location()

        self.excel_file = os.path.join(
            "extracted-zips", self.base_name, self.base_name + ".xlsx"
        )

        self.table_name = (
            "school" if self.school_type != "POSTSECSCH" else "university"
        )

    def _convert_school_type(self, school_type: str):
        match school_type.lower():
            case "public":
                return "PUBLICSCH"
            case "private":
                return "PRIVATESCH"
            case "postsecondary":
                return "POSTSECSCH"
            case _:
                raise ValueError(f"{school_type} is not a valid school type.")

    def _convert_extracted_location(self) -> str:
        # The public school and postsecondary ZIPs have a single folder inside
        # them which then contains the files. The private schools just have the
        # files, not inside a folder.

        match self.school_type:
            case "PUBLICSCH":
                return "extracted-zips"
            case "PRIVATESCH":
                return os.path.join("extracted-zips", self.base_name)
            case "POSTSECSCH":
                return "extracted-zips"
            case _:
                raise ValueError("This shouldn't happen.")

    def download(self):
        conditional_download(self.url, self.raw_file_loc)

    def extract(self):
        conditional_extract(
            self.raw_file_loc,
            self.extracted_location,
            self.excel_file,
        )

    def append_to_duckdb(self, duck_con: duckdb.DuckDBPyConnection):
        """Append the Data to DuckDB

        Args:
            duck_con (duckdb.DuckDBPyConnection): The DuckDB connection.
        """

        sql = (
            f"select *, edition: {self.year}::INT "
            f"from read_xlsx('{self.excel_file}')"
        )

        print(self.excel_file)

        if not os.path.exists(self.excel_file):
            raise FileNotFoundError("The Excel file was not found.")

        # if the table does not exist at all, add this segment to the table.
        if not table_exists(duck_con, self.table_name):
            duck_con.sql(sql).create(self.table_name)
        else:
            current_rows_present = (
                duck_con.table(self.table_name)
                .filter(f"edition = {self.year}")
                .shape[0]
            )

            # if not present, insert it.
            if current_rows_present == 0:
                duck_con.execute(f"INSERT INTO {self.table_name} " + sql)
