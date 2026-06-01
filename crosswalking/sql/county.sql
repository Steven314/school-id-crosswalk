SELECT
    state_fips: statefp,
    county_fips: countyfp,
    fips: geoid,
    county_name: namelsad,
    county_name_short: name,
    center_latitude: intptlat::DOUBLE,
    center_longitude: intptlon::DOUBLE,
    edition,
    geom
FROM geography.county
