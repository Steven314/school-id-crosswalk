import duckdb
import polars as pl

from utils.ceeb import CEEB_NCAA, CEEBCollege, CEEBHighSchool

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
    high_school: bool = False
    ncaa_hs: bool = True

    if college:
        with duckdb.connect(  # type: ignore
            "clean-data/ceeb.duckdb"
        ) as duck:
            duckdb.DuckDBPyConnection

            ceeb = CEEBCollege()
            ceeb.download()

            ceeb_data = ceeb.process()
            ceeb.append_to_duckdb(duck, ceeb_data)

    if high_school:
        with CEEBHighSchool(timeout_limit=30) as ceeb:
            data = ceeb.process()

            with duckdb.connect(  # type: ignore
                "clean-data/ceeb.duckdb"
            ) as duck:
                duckdb.DuckDBPyConnection

                ceeb.append_to_duckdb(duck, data)

    if ncaa_hs:
        # This needs to the above high school section done.
        with duckdb.connect("clean-data/ceeb-temp.duckdb") as duck:  # type: ignore
            duckdb.DuckDBPyConnection

            ceeb_codes = (
                duck.sql(
                    "select distinct ceeb_code "
                    "from school "
                    "order by ceeb_code "
                )
                .pl()["ceeb_code"]
                .to_list()
            )

        print(f"This will gather {len(ceeb_codes)} CEEB codes.")

        with CEEB_NCAA() as ceeb:
            big_data = (
                pl.from_dicts([ceeb.process(c) for c in ceeb_codes])
                .with_columns(
                    pl.col("NCAA High School Code").alias("ncaa_code"),
                    pl.col("CEEB Code").alias("ceeb_code"),
                    pl.col("High School Name").alias("name"),
                    pl.col("Address")
                    .str.extract_groups("(.*)<br>(.*)<br>(\\w\\w)  - (\\d+)")
                    .alias("address_pieces"),
                )
                .select(
                    "ncaa_code",
                    "ceeb_code",
                    "name",
                    pl.col("address_pieces").struct["1"].alias("address"),
                    pl.col("address_pieces").struct["2"].alias("city"),
                    pl.col("address_pieces").struct["3"].alias("state"),
                    pl.col("address_pieces").struct["4"].alias("zip"),
                )
            ).filter(pl.col("name").is_not_null())

            print(big_data)

            with duckdb.connect(  # type: ignore
                "clean-data/ceeb-temp.duckdb"
            ) as duck:
                duckdb.DuckDBPyConnection

                ceeb.append_to_duckdb(duck, big_data)
