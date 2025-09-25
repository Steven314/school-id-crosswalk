.PHONY: build_school_districts, build_counties, build_states, build_zctas

build_school_districts:
	python -m data_collection.us_census.school_districts

build_counties:
	python -m data_collection.us_census.counties

build_states:
	python -m data_collection.us_census.states

build_zctas:
	python -m data_collection.us_census.zip_codes

build_ipeds_hd:
	python -m data_collection.hd
