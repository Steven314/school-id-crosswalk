WITH public AS (
    SELECT 
        nces: ncessch,
        lea: leaid,
        nces_name: name,
        public_private: 'public',
        address: street,
        city: city,
        county_name: nmcnty,
        state_fips: stfip,
        state_abbr: state,
        zip: zip[1:5],
        fips: cnty,
        latitude: lat,
        longitude: lon,
        edition
    FROM nces.public
), private AS (
    SELECT 
        nces: ppin,
        nces_name: name,
        public_private: 'private',
        address: street,
        city: city,
        county_name: nmcnty,
        state_fips: stfip,
        state_abbr: state,
        zip: zip[1:5],
        fips: cnty,
        latitude: lat,
        longitude: lon,
        edition
    FROM nces.private
), geo AS (
    SELECT 
        state: name, 
        state_abbr: stusps 
    FROM geography.state
)
SELECT 
    nces,
    nces_name,
    name: nces_name
        .lower()
        .regexp_replace('\.|''|\:|,', '', 'g')
        -- this is a Python UDF that removes spaces between single letters
        .tighten() 
        .regexp_replace('-|\\|\/', ' ', 'g')
        .regexp_replace(' \(the\) ', ' ')
        .regexp_replace('\(|\)', '', 'g')
        .regexp_replace('\bsaint\b', 'st')
        .regexp_replace(' the$|^the ', '')
        .regexp_replace(' and ', ' & ', 'g')
        .regexp_replace(' (\w) & (\w) ', ' \1&\2 ')
        .regexp_replace('@', ' at ') -- add spacing
        .regexp_replace(' (no|#|number)\s?(\d+)', ' \2')
        .regexp_replace('sch ', 'school ')
        .regexp_replace('sch$', 'school')
        .regexp_replace('pcss', 'pioneer charter school of science')
        .regexp_replace('pcs', ' public charter school ')
        .regexp_replace(' pc(\s|$)', ' public charter ')
        .regexp_replace(' ps(\s|$)', ' public school ')
        .regexp_replace(' cs$', ' charter school')
        .regexp_replace('chtr', 'charter')
        .regexp_replace(' ecse$', ' early childhood education center')
        .regexp_replace(' ed(\s|$)', ' education')
        .regexp_replace('ms shs', 'middle senior high school')
        .regexp_replace(' el | el$| elem | elem$', ' elementary ')
        .regexp_replace('\bacad\b', 'academy')
        .regexp_replace(' es$', ' elementary school')
        .regexp_replace(' ms$', ' middle school')
        .regexp_replace('middle$', 'middle school')
        .regexp_replace('elementary\s?$| e school', 'elementary school')
        .regexp_replace(' h s$| high$| hi school$| h school$', ' high school')
        .regexp_replace(' hs(\s|$)', ' high school ')
        .regexp_replace(' shs$', ' senior high school')
        .regexp_replace(' jh$| jhs$', ' junior high school')
        .regexp_replace('jshs', 'junior senior high school')
        .regexp_replace('jr & sr|jr sr|jrsr', 'junior senior')
        .regexp_replace(' jr ', ' junior ')
        .regexp_replace(' sr ', ' senior ')
        .regexp_replace('([pre]?\s?k) (\d+)', '\1\2')
        .regexp_replace(' int[er]?(\s|$)|intrmd', ' intermediate ')
        .regexp_replace('twnshp', 'township')
        .regexp_replace(' co | cnty ', ' county ')
        .regexp_replace('regional safe school program', 'rssp')
        .regexp_replace('lrng?', 'learning')
        .regexp_replace('\bctr\b', 'center')
        .regexp_replace('\belc\b', 'enhanced learning center')
        .regexp_replace(' cons ', ' consolidated ')
        .regexp_replace(' const ', ' construction ')
        .regexp_replace(' educ ', ' education ')
        .regexp_replace(' prep ', ' preparatory ')
        .regexp_replace('\s{2,}', ' ', 'g')
        .trim(),
    public_private,
    address: address
        .lower()
        .regexp_replace('\.', '', 'g'),
    city: city.lower(),
    county_name,
    state_fips,
    geo.state,
    state_abbr,
    zip,
    fips,
    latitude,
    longitude,
    edition
FROM (FROM public UNION ALL BY NAME FROM private) hs
LEFT JOIN geo USING (state_abbr)
