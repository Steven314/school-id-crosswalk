from utils.duckdb import DuckDB
from utils.nces import NCES

# The Department of Education publishes a shapefile and data for each public,
# private, and higher ed institution each year. This is published in on
# antiquated webpage that was kind of hard to find.

if __name__ == "__main__":
    year = 2023

    school_types = ["public", "private", "postsecondary"]

    schools = [NCES(year, school) for school in school_types]

    with DuckDB("clean-data/nces.duckdb") as duck:
        duck.install_and_load_extension("excel", use_https=True)

        for school in schools:
            school.download()
            school.extract()
            school.append_to_duckdb(duck)
