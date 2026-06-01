SELECT 
    state_fips: statefp,
    state_abbr: stusps,
    name: name,
    center_latitude: intptlat::DOUBLE,
    center_longitude: intptlon::DOUBLE,
    edition,
    geom
FROM geography.state
