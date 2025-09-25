import duckdb
import os
from utils.duckdb import install_and_load
from utils.conditionals import conditional_download

# The National Student Clearinghouse provides a fairly recent crosswalk between
# NSC codes and the UNITIDs used in IPEDS data.
#
# It can be found at https://nscresearchcenter.org/workingwithourdata/


class NCS:
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

    def append_to_duckdb(self, duck_con: duckdb.DuckDBPyConnection) -> None:
        """Append the Data to DuckDB

        Args:
            duck_con (duckdb.DuckDBPyConnection): The DuckDB connection.
        """

        sql = (
            "select * from read_xlsx("
            f"'{self.raw_file_loc}', "
            "sheet = 'NSC_to_IPEDS_UNIT_ID'"
            ")"
        )

        if not os.path.exists(self.raw_file_loc):
            raise FileNotFoundError("The Excel file was not found.")

        duck_con.execute(f"CREATE OR REPLACE TABLE 'nsc_to_ipeds' AS ({sql})")


if __name__ == "__main__":
    with duckdb.connect(  # type: ignore
        "clean-data/ncs.duckdb"
    ) as duck:
        duckdb.DuckDBPyConnection

        install_and_load(duck, "excel", use_https=True)

        ncs = NCS()
        ncs.download()
        ncs.append_to_duckdb(duck)
