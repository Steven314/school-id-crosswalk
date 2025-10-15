from utils.ceeb import CEEB_NCAA, CEEBCollege, CEEBHighSchool
from utils.duckdb import DuckDB

# The CEEB codes for colleges/universities are four digits. The only place I
# could find them is in a PDF from the College Board.
#
# https://satsuite.collegeboard.org/media/pdf/sat-score-sends-code-list.pdf
#
# The K-12 school codes are six digits. I have not found a publicly available
# index of these. Some sites have the codes for individual states, but not
# nationwide.
#
# An NCAA website has a way that given a CEEB code you can get the name,
# address, city, state, and ZIP code.

if __name__ == "__main__":
    college: bool = False
    high_school: bool = True
    ncaa_hs: bool = False

    # for ncaa_hs:
    overwrite_duckdb: bool = False

    if college:
        with DuckDB("clean-data/ceeb.duckdb") as duck:
            ceeb = CEEBCollege()
            ceeb.download()

            ceeb_data = ceeb.process()
            ceeb.append_to_duckdb(duck, ceeb_data)

    if high_school:
        with CEEBHighSchool(timeout_limit=30) as ceeb:
            data = ceeb.process()

            with DuckDB("clean-data/ceeb.duckdb") as duck:
                ceeb.append_to_duckdb(duck, data)

    if ncaa_hs:
        # This needs the above high school section done.
        with DuckDB("clean-data/ceeb.duckdb") as duck:
            ceeb_codes = (
                duck.sql(
                    "select distinct ceeb_code from school order by ceeb_code "
                )
                .pl()["ceeb_code"]
                .to_list()
            )

        chunk_length = 50
        ceeb_codes_split = [
            ceeb_codes[i : i + chunk_length]
            for i in range(0, len(ceeb_codes), chunk_length)
        ]

        print(
            f"This will gather {len(ceeb_codes)} CEEB codes"
            + f" in {len(ceeb_codes_split)} chunks of {chunk_length} each."
        )

        for i, ceeb_code_segment in enumerate(ceeb_codes_split):
            print(i)

            ceeb = CEEB_NCAA()
            ceeb.iterate(ceeb_code_segment)
            ceeb.combine_data()
            ceeb.write_ndjson()

            if overwrite_duckdb:
                with DuckDB("clean-data/ceeb.duckdb") as duck:
                    ceeb.append_to_duckdb(duck, quiet=True)
