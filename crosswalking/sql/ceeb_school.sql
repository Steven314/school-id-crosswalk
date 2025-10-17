WITH college_board AS (
    SELECT
        ceeb,
        ceeb_name: full_name,
        address,
        city,
        state_abbr,
        zip,
        latitude,
        longitude
    FROM ceeb.school
),
ncaa AS (
    SELECT 
        ceeb: ceeb_code,
        ceeb_name: name,
        address,
        city,
        state_abbr: state,
        zip
    FROM ceeb.ncaa_school
    WHERE ncaa_code IS NOT NULL
),
ncaa_new AS (
    FROM ncaa 
    ANTI JOIN college_board USING (ceeb)
),
combined AS (
    FROM college_board 
    UNION ALL BY NAME 
    FROM ncaa_new
)
SELECT 
    ceeb,
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
    ceeb_address: address,
    address: address
        .lower()
        .regexp_replace('\.', '', 'g'),
    ceeb_city: city,
    city: city.lower(),
    state_abbr,
    zip,
    latitude,
    longitude
FROM combined
