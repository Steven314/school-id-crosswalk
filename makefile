.PHONY: build_school_districts, build_counties

build_school_districts:
	python -m data_collection.us_census.school_districts

build_counties:
	python -m data_collection.us_census.counties

