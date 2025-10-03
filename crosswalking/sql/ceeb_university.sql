SELECT
    ceeb: ceeb_code,
    ceeb_name: name,
    name: name
        .regexp_replace('A & M|A&M|Agricultural (and|&) Mechanical', 'A&M')
        .regexp_replace(' and ', ' & ', 'g')
        .regexp_replace('St\.? ', 'Saint ', 'g')
        .regexp_replace('Ft\.? ', 'Fort ', 'g')
        .regexp_replace('^The ', '')
        .regexp_replace(': |-', ' ', 'g')
        .regexp_replace('\.', '', 'g')
        .regexp_replace('\\|\/', ' ', 'g')
        .regexp_replace('^UW ', 'University of Wisconsin ')
        .regexp_replace('City University of New York', 'CUNY')
        .regexp_replace('Advanced Technical Institute', 'ATI')
        .regexp_replace('Tuscon', 'Tucson')
        .regexp_replace('Main Campus', '')
        .regexp_replace('Campus', '')
        .regexp_replace('Apply$', '')
        .regexp_replace(' at ', ' ')
        .regexp_replace('\(.*\)', '', 'g')
        .regexp_replace('\s{2,}', ' ', 'g')
        .trim(),
    state: state
FROM ceeb.university
WHERE regexp_matches(
  name,
  concat_ws(
    '|',
    '[Uu]niversity',
    'College',
    'Institute',
    'Academy',
    'Center',
    'School',
    'Conservatory',
    'Seminary',
    'District',
    'Program',
    'Project',
    'Foundation',
    'Collegium',
    'Society',
    'Association',
    'Office',
    'Union',
    'Teach',
    'Committee',
    'Search',
    'Tech',
    'Scholars',
    'Agency',
    'MBA',
    'Yeshiva',
    'Club',
    'Career',
    'Service',
    'Hospital',
    'Education',
    'SUNY',
    'LIU'
  )
) AND NOT
regexp_matches(
  name,
  concat_ws(
    '|',
    'Nav(y|al) Recruit(ing|ment) District',
    'Upward Bound Program',
    'Upward Bound',
    'Educational Talent Search'
  )
)
