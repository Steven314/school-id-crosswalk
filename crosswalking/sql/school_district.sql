SELECT 
    state_fips: statefp,
    lea: unsdlea,
    name: name,
    center_latitude: intptlat::DOUBLE,
    center_longitude: intptlon::DOUBLE,
    edition,
    geom
FROM geography.school_district
