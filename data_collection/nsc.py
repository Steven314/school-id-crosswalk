import os

from utils.conditionals import conditional_download
from utils.duckdb import DuckDB

# The National Student Clearinghouse provides a fairly recent crosswalk between
# NSC codes and the UNITIDs used in IPEDS data.
#
# It can be found at https://nscresearchcenter.org/workingwithourdata/


class NSC:
    def __init__(self):
        self.file_name = "NSC_SCHOOL_CODE_TO_IPEDS_UNIT_ID_XWALK_APR-2023.xlsx"

        self.url = "/".join(
            [
                "https://nscresearchcenter.org",
                "wp-content",
                "uploads",
                self.file_name,
            ]
        )

        self.raw_file_loc = os.path.join("raw-data", self.file_name)

    def download(self):
        conditional_download(self.url, self.raw_file_loc)

    def append_to_duckdb(self, duck: DuckDB) -> None:
        """Append the Data to DuckDB

        Args:
            duck (DuckDB): A DuckDB object.
        """

        sql = (
            "select * from read_xlsx("
            f"'{self.raw_file_loc}', "
            "sheet = 'NSC_to_IPEDS_UNIT_ID'"
            ")"
        )

        if not os.path.exists(self.raw_file_loc):
            raise FileNotFoundError("The Excel file was not found.")

        duck.create_table_query("nsc_to_ipeds", sql)


if __name__ == "__main__":
    with DuckDB("clean-data/ncs.duckdb") as duck:
        duck.install_and_load_extension("excel", use_https=True)

        nsc = NSC()
        nsc.download()
        nsc.append_to_duckdb(duck)
