import os
import zipfile

from utils.conditionals import conditional_download


class IPEDS:
    def __init__(
        self, year: str | int, table_name: str, table_suffix: str = ""
    ):
        self.year = int(year)
        self.table_name = table_name
        self.table_suffix = table_suffix
        self.combined_table_name = self.table_name + self.table_suffix

        self.base_url = "https://nces.ed.gov/ipeds/datacenter/data/"

        self.file_name = (
            f"{self.table_name.upper()}{self.year}{self.table_suffix.upper()}.zip"
            if self.year > 2022
            else f"{self.table_name.lower()}{self.year}{self.table_suffix.lower()}.zip"
        )

        self.base_name = self.file_name[:-4]

        self.url = self.base_url + self.file_name

        self.raw_file_loc = os.path.join("raw-data", self.file_name)
        self.extracted_location = os.path.join(
            "extracted-zips", self.combined_table_name
        )

        self.csv_file = os.path.join(
            self.extracted_location, self.base_name + ".csv"
        )

    def download(self):
        conditional_download(self.url, self.raw_file_loc, sleep=True)

    def extract(self):
        if not os.path.exists(self.raw_file_loc):
            raise FileNotFoundError("The ZIP file was not found.")

        if not os.path.exists(self.csv_file):
            with zipfile.ZipFile(self.raw_file_loc, "r") as z:
                z.extractall(self.extracted_location)
