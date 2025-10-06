WITH a AS (
    SELECT 
        ipeds: unitid::VARCHAR,
        opeid: opeid
          .replace('"','')
          .nullif('-2')
          .nullif(' ')
          .nullif(''),
        edition,
        multicampus: case f1systyp 
          when 1 then true 
          when 2 then false 
          else null
        end,
        ipeds_name: instnm,
        name: instnm
            .replace('"', '')
            .regexp_replace('A & M|A&M|Agricultural (and|&) Mechanical', 'A&M')
            .regexp_replace(' and ', ' & ', 'g')
            .regexp_replace('St\.? ', 'Saint ', 'g')
            .regexp_replace('Ft\.? ', 'Fort ', 'g')
            .regexp_replace('^The ', '')
            .regexp_replace(': |-', ' ', 'g')
            .regexp_replace('\.', '', 'g')
            .regexp_replace('\\|\/', ' ', 'g')
            .regexp_replace('City University of New York', 'CUNY')
            .regexp_replace('Advanced Technical Institute', 'ATI')
            .regexp_replace('Main Campus', '')
            .regexp_replace('Campus', '')
            .regexp_replace(' at ', ' ')
            .regexp_replace('\(.*\)', '', 'g')
            .regexp_replace('\s{2,}', ' ', 'g')
            .trim()
            .nullif(' '),
        address: nullif(replace(addr, '"', ''), ' '),
        city: nullif(replace(city, '"', ''), ' '),
        county_name: nullif(replace(countynm, '"', ''), ' '),
        state_abbr: nullif(replace(stabbr, '"', ''), ' '),
        zip: zip[1:5],
        state_fips: lpad(fips::VARCHAR, 2, '0'),
        fips: lpad(countycd::VARCHAR, 5, '0'),
        latitude: TRY_CAST(latitude AS DOUBLE),
        longitude: TRY_CAST(longitud AS DOUBLE)
    FROM ipeds.hd
    where sector != 0
),
states AS (
    SELECT 
      state_abbr: stusps,
      state: name
    FROM geography.state
)
SELECT DISTINCT arg_max(columns(*), edition) OVER (PARTITION BY ipeds)
FROM a
LEFT JOIN states USING (state_abbr)
ORDER BY ipeds
