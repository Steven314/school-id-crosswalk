SELECT 
    ceeb,
    nces,
    state_school_id,
    ceeb_name,
    nces_name,
    low_grade,
    high_grade,
    address: coalesce(ceeb_address, nces_address),
    city: coalesce(ceeb_city, nces_city),
    state_abbr: coalesce(ceeb.state_abbr, nces.state_abbr),
    zip: coalesce(ceeb.zip, nces.zip),
    fips,
    latitude: coalesce(ceeb.latitude, nces.latitude),
    longitude: coalesce(ceeb.longitude, nces.longitude),
    public_private,
    congressional_district,
    cbsa,
    state_legislature_lower,
    state_legislature_upper
FROM (
  FROM cb_match 
  UNION ALL BY NAME (
    SELECT ceeb, nces
    FROM unique_exact_matches
  )
)
LEFT JOIN ceeb USING (ceeb)
LEFT JOIN nces USING (nces)
ORDER BY ceeb
