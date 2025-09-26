import duckdb
import os
import pdfplumber
import re
import polars as pl
from typing import List
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
