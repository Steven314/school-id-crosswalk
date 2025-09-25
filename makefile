.PHONY: build_school_districts, build_counties, build_states

build_school_districts:
	python -m data_collection.us_census.school_districts

build_counties:
	python -m data_collection.us_census.counties

build_states:
	python -m data_collection.us_census.states
