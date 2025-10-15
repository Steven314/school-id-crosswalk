WITH states AS (
    SELECT DISTINCT 
        state: name, 
        state_abbr: stusps 
    FROM geography.state
),
schools AS (
    SELECT 
        ceeb: ceeb_code,
        ncaa_name: name,
        address: address,
        city: city,
        state_abbr: state,
        zip: zip
    FROM ceeb.ncaa_school
    WHERE ceeb_code NOT IN ('000000', '000003') AND ncaa_name IS NOT NULL
),
schools_geo AS (
    SELECT * 
    FROM schools
    LEFT JOIN states USING (state_abbr)
),
ceebs AS (
    SELECT 
        ceeb: case (ipeds is null)
          when true then lpad(ceeb, 6, '0')
          when false then lpad(ceeb, 4, '0')
        end,
        nces,
        ceeb_name: name,
        address,
        city,
        -- fix some variation in the state names
        state: case state
            when 'District Of Columbia' then 'District of Columbia'
            when 'Virgin Islands' then 'United States Virgin Islands'
            when 'Northern Mariana Islands' then 'Commonwealth of the Northern Mariana Islands'
            else state
        end,
        zip,
        latitude,
        longitude
    FROM ceeb.school
    WHERE ipeds IS NULL
),
ceebs_geo AS (
    SELECT * 
    FROM ceebs 
    LEFT JOIN states USING (state)
),
combined AS (
    SELECT 
        ceeb,
        nces,
        ceeb_name: coalesce(ceebs_geo.ceeb_name, schools_geo.ncaa_name),
        address: coalesce(ceebs_geo.address, schools_geo.address),
        city: coalesce(ceebs_geo.city, schools_geo.city),
        state: coalesce(ceebs_geo.state, schools_geo.state),
        state_abbr: coalesce(ceebs_geo.state_abbr, schools_geo.state_abbr),
        zip: coalesce(ceebs_geo.zip, schools_geo.zip),
        latitude,
        longitude
    FROM schools_geo
    FULL JOIN ceebs_geo USING (ceeb) 
)
SELECT 
    ceeb,
    nces,
    ceeb_name,
    name: ceeb_name
        .lower()
        .regexp_replace('\.|''|\:|,', '', 'g')
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
        .regexp_replace('pcss', 'pioneer charter school of science')
        .regexp_replace('pcs', ' public charter school ')
        .regexp_replace(' pc(\s|$)', ' public charter ')
        .regexp_replace(' ps(\s|$)', ' public school ')
        .regexp_replace(' cs$', ' charter school')
        .regexp_replace('chtr', 'charter')
        .regexp_replace(' ecse$', ' early childhood education center')
        .regexp_replace(' ed(\s|$)', ' education')
        .regexp_replace('ms shs', 'middle senior high school')
        .regexp_replace('el$|elementary$| e school', ' elementary school')
        .regexp_replace(' acad$', ' academy')
        .regexp_replace('middle$', 'middle school')
        .regexp_replace(' hs$', ' high school')
        .regexp_replace(' jh$| jhs$', ' junior high school')
        .regexp_replace(' shs$', ' senior high school')
        .regexp_replace('jshs', 'junior senior high school')
        .regexp_replace('jr & sr|jr sr|jrsr', 'junior senior')
        .regexp_replace(' jr ', ' junior ')
        .regexp_replace(' sr ', ' senior ')
        .regexp_replace('([pre]?\s?k) (\d+)', '\1\2')
        .regexp_replace(' int[er]?(\s|$)|intrmd', ' intermediate ')
        .regexp_replace('juvenile justice academy for academic excellence', 'jjaep')
        .regexp_replace('regional safe school program', 'rssp')
        .regexp_replace('lrng?', 'learning')
        .regexp_replace('\bctr\b|\bcntr\b', 'center')
        .regexp_replace('\belc\b', 'enhanced learning center')
        .regexp_replace(' co | cnty ', ' county ')
        .regexp_replace(' cons ', ' consolidated ')
        .regexp_replace(' const ', ' construction ')
        .regexp_replace(' educ ', ' education ')
        .regexp_replace(' prep ', ' preparatory ')
        .regexp_replace('\s{2,}', ' ', 'g')
        .trim(),
    address: address
        .lower()
        .regexp_replace('\.', '', 'g'),
    city: city.lower(),
    state,
    state_abbr,
    zip,
    latitude,
    longitude
FROM combined
