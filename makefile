# All Geographies
.PHONY: build_geography
build_geography: build_school_districts build_counties build_states build_zctas

# School Districts
.PHONY: build_school_districts
build_school_districts:
	python -m data_collection.us_census.school_districts

# Counties
.PHONY: build_counties
build_counties:
	python -m data_collection.us_census.counties

# States
.PHONY: build_states
build_states:
	python -m data_collection.us_census.states

# ZCTAs
.PHONY: build_zctas
build_zctas: 
	python -m data_collection.us_census.zip_codes

# IPEDS HD
.PHONY: build_ipeds_hd
build_ipeds_hd:
	python -m data_collection.hd

# NCES
.PHONY: build_nces
build_nces:
	python -m data_collection.nces

# NSC
.PHONY: build_nsc
build_ncs:
	python -m data_collection.nsc

# CEEB
.PHONY: build_ceeb
build_ceeb:
	python -m data_collection.ceeb
