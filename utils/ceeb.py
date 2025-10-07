import json
import os
import re
import time
from io import StringIO
from typing import List

import pdfplumber
import polars as pl
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.wait import WebDriverWait

from utils.conditionals import conditional_download
from utils.duckdb import DuckDB

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

    def append_to_duckdb(self, duck: DuckDB, data: pl.DataFrame):
        # Data is used implicitly by DuckDB here.

        sql = "SELECT * FROM data"

        # Because of a scope issue, the DuckDB wrapper must be bypassed.
        duck.duck.execute(
            f"create or replace table {self.table_name} as ({sql})"
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

        self.storage_path = os.path.join("extracted-zips", "ceeb")

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
        self.file_path = os.path.join(
            self.storage_path, self.state_name + ".txt"
        )

        # The Marshall Islands apparently have no results which results in an
        # error. Skip that case.

        if self.state_name == "Marshall Islands":
            return

        if not os.path.exists(self.file_path):
            self.click_submit()

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

            if not os.path.exists(self.storage_path):
                os.mkdir(self.storage_path)

            with open(self.file_path, "w") as f:
                f.write(table_contents)

        else:
            with open(self.file_path, "r") as f:
                table_contents = f.read()

        # Convert it from "<name>\n<number>\n" to "<name>|<number>\n".
        # This is essentially making it a pipe-separated file as a single
        # string.
        pipe_table: str = re.sub(
            r"(.*)\n(\d+)\n?",
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

            self.get_table()

            # wait a bit to be safe
            time.sleep(0.5)

        return self.collect_data()

    def append_to_duckdb(self, duck: DuckDB, data: pl.DataFrame):
        sql = "SELECT * FROM data"

        # Because of a scope issue, the DuckDB wrapper must be bypassed.
        duck.duck.execute(
            f"create or replace table {self.table_name} as ({sql})"
        )


class CEEB_NCAA:
    def __init__(self, timeout_limit: int = 20):
        # initial URL of the page
        self.url = (
            "https://web3.ncaa.org/"
            "hsportal/"
            "exec/"
            "hsAction?hsActionSubmit=searchHighSchool"
        )

        self.timeout_limit = timeout_limit

        self.storage_path = os.path.join("extracted-zips", "ceeb_ncaa")

        if not os.path.exists(self.storage_path):
            os.mkdir(self.storage_path)

        self.table_name = "ncaa_school"

    def __enter__(self):
        self.driver = webdriver.Chrome()
        self.driver.get(self.url)

        return self

    def __exit__(self, exc_type, exc_value, traceback):  # type: ignore
        self.driver.quit()

    def search_ceeb_code(self, ceeb: str):
        wait = WebDriverWait(
            self.driver,
            timeout=self.timeout_limit,
            poll_frequency=0.25,
            ignored_exceptions=[Exception],
        )

        # find the text box
        ceeb_text_box = wait.until(
            EC.presence_of_element_located((By.ID, "ceebCodeOnCrsDispId"))
        )

        ceeb_text_box.clear()

        # enter the CEEB code
        ActionChains(self.driver).send_keys_to_element(
            ceeb_text_box, ceeb
        ).perform()

        # hit "Search"
        self.driver.find_element(by=By.NAME, value="hsActionSubmit").click()

    def pull_table(self, ceeb: str) -> dict[str, str | None]:
        wait = WebDriverWait(
            self.driver,
            timeout=self.timeout_limit,
            poll_frequency=0.25,
            ignored_exceptions=[Exception],
        )

        table = wait.until(
            EC.any_of(
                EC.presence_of_element_located(
                    (
                        By.CSS_SELECTOR,
                        "div.panelsStayOpenHsSummary.accordion-collapse.collapse.show",
                    )
                ),
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "span.error")
                ),
            )
        )

        try:
            table = table.find_element(  # type: ignore
                by=By.CSS_SELECTOR,
                value="table.table.table-sm.table-bordered.border-primary",
            )

            rows = BeautifulSoup(
                str(table.get_attribute("innerHTML")),  # type: ignore
                "html.parser",
            ).find_all("tr")

            data: dict[str, str | None] = dict(
                [
                    [
                        td.get_text(separator="<br>", strip=True)  # type: ignore
                        for td in row.find_all("td")  # type: ignore
                    ]
                    for row in rows[1:5]
                ]
            )

        except NoSuchElementException:
            data: dict[str, str | None] = dict(
                {
                    "NCAA High School Code": None,
                    "CEEB Code": ceeb,
                    "High School Name": None,
                    "Address": None,
                }
            )

        return data

    def return_to_search(self):
        self.driver.back()

    def check_ceeb_data(self, file: str) -> bool:
        return os.path.exists(file)

    def load_ceeb_data(self, file: str) -> dict[str, str]:
        with open(file, "r") as f:
            data = json.load(f)

        return data

    def save_ceeb_data(self, file: str, data: dict[str, str]):
        with open(file, "w") as f:
            f.write(json.dumps(data))

    def process(self, ceeb: str):
        file = os.path.join(self.storage_path, ceeb + ".json")

        if not self.check_ceeb_data(file):
            # if the data is not known, download it.
            self.search_ceeb_code(ceeb)

            data = self.pull_table(ceeb)

            self.save_ceeb_data(file, data)  # type: ignore
            self.return_to_search()
        else:
            # if it is known, load it.
            data = self.load_ceeb_data(file)

        return data

    def append_to_duckdb(self, duck: DuckDB, data: pl.DataFrame):
        sql = "SELECT * FROM data"

        # Because of a scope issue, the DuckDB wrapper must be bypassed.
        duck.duck.execute(
            f"create or replace table {self.table_name} as ({sql})"
        )
