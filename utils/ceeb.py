import os
import re
import time
from io import StringIO
from typing import List

import duckdb
import pdfplumber
import polars as pl
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.wait import WebDriverWait

from utils.conditionals import conditional_download

os.environ["DC_STATEHOOD"] = "1"
import us  # type: ignore


class CEEBCollege:
    def __init__(self):
        self.base_url = "https://satsuite.collegeboard.org/media/pdf/"
        self.base_name = "sat-score-sends-code-list.pdf"
        self.url = self.base_url + self.base_name

        self.loc = os.path.join("raw-data", self.base_name)

        self.states = [s.name.upper() for s in us.STATES]
        self.states.sort()

        self.states_clean = [
            s.title().replace("Of", "of") for s in self.states
        ]

        self.table_name = "university"

    def download(self):
        conditional_download(self.url, self.loc)

    def process(self):
        pdf = pdfplumber.open(self.loc)

        # Using text flow here helps get the word wrapping in the right order.
        # The first page (page 0) is a cover page.
        text = [p.extract_text(use_text_flow=True) for p in pdf.pages[1:]]

        text_for_removal = "|".join([r"2025 SAT Score Sends Code List \d+"])

        joined_text = re.sub(
            text_for_removal,
            " ",
            " ".join(text).replace("\n", " ").replace("’", "'"),
        )

        headers = "|".join(
            [
                r"U\.S\. COLLEGES AND UNIVERSITIES",
                r"COLLEGES IN U\.S\. TERRITORIES AND PUERTO RICO",
                r"COLLEGES AND UNIVERSITIES OUTSIDE THE U\.S\.",
                r"NATIONAL SCHOLARSHIP PROGRAMS OR OTHER EDUCATION PROVIDERS",
                r"SCHOLARSHIP PROGRAMS OR OTHER EDUCATION PROVIDERS BY STATE OR TERRITORY",
                r"SCHOLARSHIP PROGRAMS OR OTHER EDUCATION PROVIDERS OUTSIDE THE U\.S\.",
            ]
        )

        split_text = re.split(headers, joined_text)

        # We only need the US colleges and universities.
        split_states = re.split("|".join(self.states), split_text[1])[1:]

        regex = r"((?:[A-z\s\'\-\:\&\.\(\)\/]+))\s(\d+)\s?"

        result: List[dict[str, str]] = []
        for i in range(len(split_states)):
            matches: List[str] = re.findall(regex, split_states[i])

            for group, num in matches:
                result.append(
                    {
                        "state": self.states_clean[i],
                        "name": group.strip(),
                        "ceeb_code": num.strip(),
                    }
                )

        data = pl.DataFrame(result).sort("state", "name")

        return data

    def append_to_duckdb(
        self, duck_con: duckdb.DuckDBPyConnection, data: pl.DataFrame
    ):
        # Data is used implicitly by DuckDB here.

        sql = "SELECT * FROM data"

        duck_con.execute(
            f"CREATE OR REPLACE TABLE {self.table_name} AS ({sql})"
        )


class CEEBHighSchool:
    def __init__(self, timeout_limit: int = 20):
        # initial URL of the page
        self.url = (
            "https://satsuite.collegeboard.org/"
            "k12-educators/"
            "tools-resources/"
            "k12-school-code-search"
        )

        self.timeout_limit = timeout_limit

        # an index for the state selected.
        self.state_index = 0
        self.n_states = 57

        # create a list of empty data frames
        self.data: List[pl.DataFrame] = [
            pl.DataFrame(
                schema={
                    "name": pl.String,
                    "ceeb_code": pl.String,
                    "state": pl.String,
                }
            )
        ] * self.n_states

        self.table_name = "school"

    def __enter__(self):
        self.driver = webdriver.Chrome()
        self.driver.get(self.url)

        self.wait = WebDriverWait(
            self.driver,
            timeout=self.timeout_limit,
            poll_frequency=1,
            ignored_exceptions=[Exception],
        )

        # dismiss the cookies popup
        cookies_reject = self.wait.until(
            EC.element_to_be_clickable((By.ID, "onetrust-reject-all-handler"))
        )
        cookies_reject.click()

        # reload manually to avoid the weird refresh issue when rejecting
        # cookies.
        self.driver.get(self.url)

        return self

    def __exit__(self, exc_type, exc_value, traceback):  # type: ignore
        self.driver.quit()

    def select_dropdown(self):
        wait = WebDriverWait(
            self.driver,
            timeout=self.timeout_limit,
            poll_frequency=1,
            ignored_exceptions=[Exception],
        )

        dropdown_menu = wait.until(
            EC.presence_of_element_located((By.ID, "apricot_select_4"))
        )

        self.select = Select(dropdown_menu)

    def choose_next_state(self):
        self.state_index += 1
        self.select.select_by_index(self.state_index)

        print(f"choosing index: {self.state_index=}")

        self.state_name = self.select.first_selected_option.text

    def click_submit(self):
        self.driver.find_element(By.CLASS_NAME, "cb-btn-primary").click()

    def get_table(self):
        # The Marshall Islands apparently have no results which results in an
        # error. Skip that case.

        if self.state_name == "Marshall Islands":
            return

        wait = WebDriverWait(
            self.driver,
            timeout=self.timeout_limit,  # the larger states take longer.
            poll_frequency=1,
            ignored_exceptions=[Exception],
        )

        # wait until the table loads.
        table = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "ul.cb-text-list.cb-text-list-feature")
            )
        )

        # the text out of the table.
        table_contents = str(table.get_attribute("innerText"))  # type: ignore

        # Convert it from "<name>\n<number>\n" to "<name>|<number>\n".
        # This is essentially making it a pipe-separated file as a single
        # string.
        pipe_table: str = re.sub(
            r"(\w+)\n(\d+)\n?",
            r"\1|\2\n",
            table_contents,
        )

        # Convert it to polars and put it in the dataframe. The minus one is
        # because the website has 1-based indexing and Python is 0-based.
        self.data[self.state_index - 1] = pl.read_csv(
            StringIO(pipe_table),
            schema={"name": pl.String, "ceeb_code": pl.String},
            has_header=False,
            separator="|",
        ).with_columns(state=pl.lit(self.state_name))

    def collect_data(self) -> pl.DataFrame:
        return pl.concat(self.data)

    def process(self):
        while self.state_index < self.n_states:
            print(f"while loop: {self.state_index=}")
            self.select_dropdown()

            # state index is iterated in choosing the next state
            self.choose_next_state()

            self.click_submit()
            self.get_table()

            # wait a bit to be safe
            time.sleep(0.5)

        return self.collect_data()

    def append_to_duckdb(
        self, duck_con: duckdb.DuckDBPyConnection, data: pl.DataFrame
    ):
        sql = "SELECT * FROM data"

        duck_con.execute(
            f"CREATE OR REPLACE TABLE {self.table_name} AS ({sql})"
        )
