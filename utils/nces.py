import os
import re
import time
from collections import defaultdict
from typing import List

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from utils.conditionals import conditional_download, conditional_extract
from utils.duckdb import DuckDB

os.environ["DC_STATEHOOD"] = "1"
import us  # type: ignore


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

        self.table_name = school_type.lower()

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

    def append_to_duckdb(self, duck: DuckDB):
        """Append the Data to DuckDB

        Args:
            duck (DuckDB): A DuckDB object.
        """

        sql = (
            "select if("
            "typeof(columns(*)) = 'VARCHAR', "
            "cast_to_type("
            "if(cast_to_type(columns(*), '') in ('M', 'N'), null, columns(*)),"
            "columns(*)"
            "), "
            "columns(*)"
            "), "
            f"edition: {self.year}::INT "
            f"from read_xlsx('{self.excel_file}') "
            "order by cnty"
        )

        if not os.path.exists(self.excel_file):
            raise FileNotFoundError("The Excel file was not found.")

        # if the table does not exist at all, add this segment to the table.
        if not duck.table_exists(self.table_name):
            duck.sql(sql).create(self.table_name)
        else:
            current_rows_present = (
                duck.table(self.table_name)
                .filter(f"edition = {self.year}")
                .shape[0]
            )

            # if not present, insert it.
            if current_rows_present == 0:
                duck.execute(f"INSERT INTO {self.table_name} " + sql)


class NeoNCES:
    def __init__(self, duck: DuckDB):
        self.public_url = "https://nces.ed.gov/ccd/schoolsearch/"
        self.private_url = (
            "https://nces.ed.gov/surveys/pss/privateschoolsearch/"
        )

        self.driver = webdriver.Chrome()

        self.storage_path = os.path.join("extracted-zips", "nces")
        if not os.path.exists(self.storage_path):
            os.mkdir(self.storage_path)

        self.state_fips = [state.fips for state in us.STATES]

        self.duck = duck

        self.table_name = "school"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):  # type: ignore
        self.close()

    def close(self):
        self.driver.quit()

    def go_to_state(self, state_fips: str, public_private: str = "public"):
        page = f"school_list.asp?State={state_fips}"

        if public_private == "public":
            self.driver.get(self.public_url + page)
        elif public_private == "private":
            self.driver.get(self.private_url + page)
        else:
            raise ValueError("pubic_private is not right.")

    def download_excel(self, public_private: str, fips: str):
        """
        These aren't really Excel files but HTML files with an Excel file
        extension.
        """

        # this opens a new window
        self.driver.find_element(By.CLASS_NAME, "excelclass").click()

        time.sleep(1)

        # switch to the new window
        self.driver.switch_to.window(self.driver.window_handles[-1])

        buttons = WebDriverWait(self.driver, 10).until(
            EC.visibility_of_all_elements_located((By.TAG_NAME, "a"))
        )

        time.sleep(1)

        file_name = (
            "ncesdata_"
            + re.search("filename=(.*)", self.driver.current_url).group(1)  # type: ignore
            + ".xls"
        )

        file_path = os.path.join(
            f"extracted-zips/nces/{public_private}_{fips}.html"
        )

        # hit the download button, but only if the file does not already exist.
        if not os.path.exists(file_path):
            buttons[0].click()

            time.sleep(2)  # allow time for the download

            # move the downloaded file to the right folder.
            os.rename(
                os.path.expanduser(f"~/Downloads/{file_name}"), file_path
            )

        # close the little window
        buttons[-1].click()

        # go back to main window
        self.driver.switch_to.window(self.driver.window_handles[0])

    def iterate(self, public_private: str):
        for state in self.state_fips:
            file = os.path.join(
                f"extracted-zips/nces/{public_private}_{state}.xls"
            )

            # if it exists, skip it.
            if not os.path.exists(file):
                self.go_to_state(str(state), public_private)
                self.download_excel(public_private, str(state))

                time.sleep(1)

    def gather(self):
        public_tables: List[pd.DataFrame] = []
        private_tables: List[pd.DataFrame] = []

        for file in os.listdir(self.storage_path):
            with open(os.path.join(self.storage_path, file)) as f:
                # skipping the file header.
                skip: int = 0
                if "public" in file:
                    skip = 5
                if "private" in file:
                    skip = 4

                table: pd.DataFrame = pd.read_html(  # type: ignore
                    io=f,
                    skiprows=skip,
                    header=0,
                    # force all columns to be strings
                    converters=defaultdict(lambda: str),
                )[0]

                if "public" in file:
                    public_tables.append(table)
                if "private" in file:
                    private_tables.append(table)

        # combine
        public_df = pd.concat(public_tables)  # type: ignore # noqa: F841
        private_df = pd.concat(private_tables)  # type: ignore # noqa: F841

        self.data = self.duck.duck.sql(
            """
            with public as (
                select 
                    nces: "NCES School ID",
                    state_school_id: "State School ID",
                    lea: "NCES District ID",
                    state_district_id: "State District ID",
                    low_grade: "Low Grade",
                    high_grade: "High Grade",
                    nces_name: "School Name",
                    district: "District",
                    county_name: "County Name",
                    address: "Street Address",
                    city: "City",
                    state_abbr: "State",
                    zip: "ZIP"
                from public_df
            ),
            private as (
                select 
                    nces: '0000' || PSS_SCHOOL_ID,
                    nces_name: PSS_INST,
                    low_grade: LoGrade,
                    high_grade: HiGrade,
                    address: PSS_ADDRESS,
                    city: PSS_CITY,
                    fips: PSS_COUNTY_NO,
                    state_abbr: PSS_STABB,
                    state_fips: PSS_FIPS,
                    zip: PSS_ZIP5,
                    county_name: PSS_COUNTY_NAME
                from private_df
            )
            from public 
            union all by name 
            from private 
            order by nces
            """
        ).pl()

    def append_to_duckdb(self):
        data = self.data  # type: ignore # noqa: F841

        self.duck.duck.execute(
            f"CREATE OR REPLACE TABLE {self.table_name} AS (FROM data)"
        )
