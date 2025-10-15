import json
import os
import re
import time
from typing import Any, List

import pdfplumber
import polars as pl
from bs4 import BeautifulSoup
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.wait import WebDriverWait
from seleniumwire import webdriver

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
    def __init__(self, duck: DuckDB, timeout_limit: int = 20):
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

        self.storage_path = os.path.join("extracted-zips", "ceeb")

        self.table_name = "school"

        self.duck = duck

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

    def pull_json(self):
        self.file_path = os.path.join(
            self.storage_path, self.state_name + ".json"
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

            # just to make the process wait for it to finish loading.
            result_wait = wait.until(
                # EC.presence_of_element_located((By.CLASS_NAME, "col-xs-6"))
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "ul.cb-text-list.cb-text-list-feature")
                )
            )

            result_number = int(result_wait.get_property("childElementCount"))  # type: ignore
            print(result_number)

            # gather the responses as JSON
            responses: List[dict[str, Any]] = []
            for request in self.driver.requests:
                if (
                    "https://organization.cds-prod.collegeboard.org/pine/aisearch"
                    in request.url
                ):
                    responses.append(
                        json.loads(request.response.body.decode())  # type: ignore
                    )

            # clear the requests list for the next iteration
            del self.driver.requests

            # save the data as a JSON file.
            if not os.path.exists(self.storage_path):
                os.mkdir(self.storage_path)

            with open(self.file_path, "w") as f:
                json.dump(responses, f, indent=2)

    def collect_data(self) -> pl.DataFrame:
        # convert build the table from JSON with DuckDB
        sql = (
            "with unnested as ("
            "  select "
            "    var: unnest(hits.hits)._source, "
            "    state: parse_filename(filename, true) "
            f"  from '{self.storage_path.replace('\\', '/')}/*.json'"
            "), "
            "pieces as ("
            "  select "
            "    ceeb: var.ais[1].ai_code,"
            "    nces: lpad(var.org_nces_sch_id, 12, '0'),"
            "    ipeds: var.org_ipeds_id,"
            "    full_name: var.org_full_name,"
            "    name: var.di_name,"
            "    short_name: var.org_short_name,"
            "    abbreviated_name: var.org_abbrev_name,"
            "    address: var.org_street_addr1,"
            "    city: var.org_city,"
            "    state,"
            "    state_abbr: var.org_state_cd,"
            "    country: var.org_country_iso_cd,"
            "    zip: var.org_zip5,"
            "    latitude: var.org_geo.lat,"
            "    longitude: var.org_geo.lon,"
            "    updated: make_timestamp(var.last_update_dt::BIGINT * 1000)"
            "  from unnested"
            ") "
            "from pieces"
        )

        self.data = self.duck.sql(sql).pl()

        return self.data

    def process(self):
        while self.state_index < self.n_states:
            print(f"while loop: {self.state_index=}")
            self.select_dropdown()

            # state index is iterated in choosing the next state
            self.choose_next_state()

            self.pull_json()

            # wait a bit to be safe
            time.sleep(0.5)

    def append_to_duckdb(self):
        data = self.data  # type: ignore # noqa: F841
        sql = "SELECT * FROM data"

        # Because of a scope issue, the DuckDB wrapper must be bypassed.
        self.duck.duck.execute(
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

        self.storage_file = os.path.join("extracted-zips", "ceeb_ncaa.ndjson")

        if not os.path.exists("extracted-zips"):
            os.mkdir("extracted-zips")

        if os.path.exists(self.storage_file):
            self.old_data = pl.read_ndjson(
                self.storage_file,
                schema=dict(
                    {
                        "ncaa_code": pl.String,
                        "ceeb_code": pl.String,
                        "name": pl.String,
                        "address": pl.String,
                        "city": pl.String,
                        "state": pl.String,
                        "zip": pl.String,
                        "message": pl.String,
                    }
                ),
            )
            self.processed_ceebs: List[str] = self.old_data[
                "ceeb_code"
            ].to_list()

        else:
            self.processed_ceebs: List[str] = []

        self.table_name = "ncaa_school"

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

        # clear any prior input
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
            # this will intentionally fail if it is not found
            table = table.find_element(  # type: ignore
                by=By.CSS_SELECTOR,
                value="table.table.table-sm.table-bordered.border-primary",
            )

            rows = BeautifulSoup(
                str(table.get_attribute("innerHTML")),  # type: ignore
                "html.parser",
            ).find_all("tr")

            data_pre: dict[str, str] = dict(
                [
                    [
                        td.get_text(separator="<br>", strip=True)  # type: ignore
                        for td in row.find_all("td")  # type: ignore
                    ]
                    for row in rows[1:5]
                ]
            )

            address_parts: re.Match[str] = re.match(
                "(.*)<br>(.*)<br>(\\w\\w)  - (\\d+)",
                str(data_pre.get("Address")),
            )  # type: ignore

            # transform the address box to address, city, state, zip separately
            data: dict[str, str | None] = dict(
                {
                    "ncaa_code": data_pre.get("NCAA High School Code"),
                    "ceeb_code": data_pre.get("CEEB Code"),
                    "name": data_pre.get("High School Name"),
                    "address": address_parts.group(1),
                    "city": address_parts.group(2),
                    "state": address_parts.group(3),
                    "zip": address_parts.group(4),
                    "message": None,
                }
            )

        except NoSuchElementException:
            # capture the error message in case it shows anything interesting
            error = self.driver.find_elements(By.CSS_SELECTOR, "span.error")[0]  # type: ignore

            data: dict[str, str | None] = dict(
                {
                    "ncaa_code": None,
                    "ceeb_code": ceeb,
                    "name": None,
                    "address": None,
                    "city": None,
                    "state": None,
                    "zip": None,
                    "message": error.get_attribute("innerText"),  # type: ignore
                }
            )

        return data

    def return_to_search(self):
        self.driver.back()

    def process(self, ceeb: str):
        # if the data is not known, download it and append it to the list
        start = time.time()
        self.search_ceeb_code(ceeb)

        data = self.pull_table(ceeb)

        self.new_data_list.append(data)

        self.return_to_search()

        end = time.time()
        time_taken = end - start

        print(f"CEEB {ceeb} in {time_taken:.02f} seconds.")

    def iterate(self, ceebs: List[str]):
        self.new_data_list: List[dict[str, str | None]] = []

        ceebs_needed = list(
            filter(lambda x: x not in self.processed_ceebs, ceebs)
        )

        print(f"This will fetch {len(ceebs_needed)} CEEB codes from the web.")

        if len(ceebs_needed) != 0:
            self.driver = webdriver.Chrome()
            self.driver.get(self.url)

            for ceeb in ceebs_needed:
                self.process(ceeb)

            self.driver.close()

        self.new_data = pl.from_dicts(
            self.new_data_list,
            schema=dict(
                {
                    "ncaa_code": None,
                    "ceeb_code": None,
                    "name": None,
                    "address": None,
                    "city": None,
                    "state": None,
                    "zip": None,
                    "message": None,
                }
            ),
        )

    def combine_data(self):
        if hasattr(self, "old_data"):
            self.combined_data = self.old_data.vstack(self.new_data)
        else:
            self.combined_data = self.new_data

    def write_ndjson(self):
        self.combined_data.sort("ceeb_code").write_ndjson(self.storage_file)

    def append_to_duckdb(self, duck: DuckDB, quiet: bool = True):
        data = pl.read_ndjson(self.storage_file)

        if not quiet:
            print(data)

        sql = "SELECT * FROM data"

        # Because of a scope issue, the DuckDB wrapper must be bypassed.
        duck.duck.execute(
            f"create or replace table {self.table_name} as ({sql})"
        )
