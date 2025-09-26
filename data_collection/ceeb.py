import duckdb
from utils.ceeb import CEEBCollege

# The CEEB codes for colleges/universities are four digits. The only place I
# could find them is in a PDF from the College Board.
#
# https://satsuite.collegeboard.org/media/pdf/sat-score-sends-code-list.pdf
#
# The K-12 school codes are six digits. I have not found a publicly available
# index of these. Some sites have the codes for individual states, but not
# nationwide.
#
# On the College Board's look-up tool you can enter a state and it will return
# all the codes for that state. I suppose you could control a web browser via
# Selenium that picks a state from the dropdown, hits 'submit', copies the
# table, and repeats.

if __name__ == "__main__":
    with duckdb.connect(  # type: ignore
        "clean-data/ceeb.duckdb"
    ) as duck:
        duckdb.DuckDBPyConnection

        ceeb = CEEBCollege()
        ceeb.download()

        ceeb_data = ceeb.process()
        ceeb.append_to_duckdb(duck, ceeb_data)
