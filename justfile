# Open a DuckDB UI w/ :memory:
duck:
    duckdb -ui 

# Build all of the geography
build-geography: build-school-districts build-counties build-states build-zctas

# Build the school districts geography
build-school-districts:
    python -m data_collection.us_census.school_districts

# Build the county geography
build-counties:
    python -m data_collection.us_census.counties

# Build the state geography
build-states:
    python -m data_collection.us_census.states

# Build the ZCTA geography
build-zctas:
    python -m data_collection.us_census.zip_codes

# Build the IPEDS HD table
build-ipeds-hd:
    python -m data_collection.hd

# Build the NCES data
build-nces:
    python -m data_collection.nces

# Build the NSC data
build-nsc:
    python -m data_collection.nsc

# Build the CEEB data
build-ceeb:
    python -m data_collection.ceeb
    

# Create the Crosswalks ('universities' or 'schools')
walk LEVEL:
    python -m crosswalking.{{LEVEL}}
