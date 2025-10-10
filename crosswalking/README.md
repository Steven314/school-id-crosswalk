# Crosswalk Information

## School Districts

These are identified by their LEA number (Local Education Agency).
The Census data uses this identifier.
The NCES data also has this identifier available for school-level data.

## Individual Schools

Individuals schools have multiple identifiers.
From the College Board they have CEEB codes (which I've called `ceeb`), and from NCES they have another identifier (which I've called `nces`).

This is the difficult part.
There is no 100% factual reference between these identification systems (that I could find).
Mark Davenport's is probably the best available resource, but I would like to do this only based on original government/organization data files.
Davenport links to a [PDF](https://brevard.edu/wp-content/uploads/2020/08/ceeb-lookup-masterlist.pdf) from 2020 which has CEEB codes with name, state, and ZIP code, but I don't know if that is reliable or very up-to-date.
(It is also 1,742 pages long :expressionless:)
Davenport also link to an NCAA webpage that can give the ZIP, state, city, street address, and name from a CEEB code.
Pulling all of those is a pain and takes a long time.

### Issues

- There are a number of CEEB codes that correspond to schools of the same name
  in the same state. For example, there are four Lighthouse Christian Academies
  in California, two John Jay High Schools in New York, and two North High
  Schools in Ohio. That doesn't even consider slight variations, such as
  "Academy" versus "School".
  - Having a second data point associated with a CEEB code, such as ZIP codes
    from the NCAA site, would be useful.
- The NCAA site doesn't have every CEEB code that is found on the College Board
  site. That may not be much of an issue since the College Board has many codes
  for places other than high schools, which is mainly what I'm after.
- Pulling from the NCAA site takes a very long time. It takes a little under a
  second to run each query and there over 47,000 queries :neutral_face:.
  - This ended up being a multi-day process to slowly pull all the records and
    monitor it.
  - About 45% of College Board codes were found on the NCAA site.

### Matching Strategy

I chose an iterative strategy for matching the records.
Each step utilizes only the records which did not match in the previous step.

### Adjustments

#### General

- Some of the states had a slightly different representation between the NCAA
  data and the geometry data. These were unified to the geometry data form.
- Dots and dashes were removed.
- `and` was converted to `&`.
- `@` was converted to `at`.
- Leading or trailing `the` were removed.
- Special characters were removed or replaced with spaces.
- Some schools have a number associated with them. The number of signifier (`#`, `no`, `number`) was removed.
- `saint` was converted to `st`.
- Extra spaces were removed.
  - This includes squashing initials, for example, `j j a e p` to `jjaep`.
- Many schools, especially in the NCES data, abbreviated parts of the
  school name. These were expanded to say the full name where possible.
  - There were many such situations across the various types of schools.
  - Most of the adjustments are related to this.

#### Specific

Some of the changes above likely only altered a couple records, but their nature was still general.

## Higher Educational Institutions

These have three identification systems: IPEDS, NSC, and CEEB codes.
Converting between IPEDS and NSC is already done via the NSC table from NSC themselves.
Although, the NSC table does not always have historical IPEDS codes that are no longer in use.
Converting between those two and CEEB is more difficult.
It has the same problems as with the high schools, but should not be as difficult since there are fewer institutions.

There are a number of institutions under the same name in the same state.
Many of these are multi-location technical colleges or similar.
There are 138 total duplicates based on the IPEDS HD table.
I checked a few of these and they have been closed for some time (at least that location).
As the most recent IPEDS appearance of them was not the most recent edition, I'm guessing that many of these are institutions that have closed.

It is common for a single institution to have multiple CEEB codes which correspond to different parts of the university.
Another difficulty is when a college/university undergoes a name change.

Many older institutions are available in the IPEDS data, but not in the CEEB data.

### Adjustments

Before any adjustments the exact match rate is about 55% of CEEB codes.
After the below adjustments the rate is almost 76% of included CEEB codes.

#### General

These are the alterations made to the names in IPEDS and CEEB data to unify common variations.
Most of these are to allow for more exact matches.
By increasing the number of exact matches through basic changes, this reduces the potential for errors in the fuzzy matching.

- `A&M`, `A & M`, and `Agricultural and Mechanical` were unified to `A&M`.
- ` and ` was converted to ` & `. It seems that IPEDS often has '&' instead of
  'and' while the College Board prefers the opposite.
- `St.` and `Saint` were unified to `Saint`.
- Any leading `The` were removed.
- Any slashes, `\`, were removed.
- The CEEB names prefer to designate a campus with the style `Main: Branch`
  while IPEDS prefers `Main-Branch`. The use of `:` and `-` conflicts in places
  where it is actually part of the name, so all instances of `:` and `-` were
  removed and replaced with a space. Also, extraneous spaces were removed.
- Sometimes initials have a `.` as in `George C. Wallace` as opposed to
  `George C Wallace`. Dots are removed and this initials are unified to the
  latter format.
- `Campus` shows up a fair amount in the IPEDS names, but not in the CEEB names.
  It was removed for both.
- `at` is often used for branch campuses in the CEEB names, but not in the IPEDS
  names. It was removed for both.
- `Ft` is sometimes used in the CEEB names for `Fort`. These were unified to
  `Fort`.
- Sometimes a CEEB name has an abbreviate name in parentheses. The IPEDS names only rarely have parentheses and it is usually for a `The` prefix.

#### Specific

- In the CEEB name some of the University of Wisconsin branch colleges are
  named with a `UW` instead of the full name.
- The 'City University of New York' was be reduced to CUNY.
- In the CEEB names the 'Advanced Technical Institute' is listed by ATI. These
  will be unified as `ATI`. (I don't think this actually helps anything.)
- In IPEDS name `Arizona College of Nursing Tucson` its CEEB counterpart has a
  typo. (This creates one additional exact match.)
- For some reason the California State University CEEB names have `Apply` on the
  end of them. That was removed.
- Texas A&M University's main campus has the city attached in the IPEDS data,
  but not the CEEB data. This causes mismatches with the branch campuses. The
  main campus had `College Station` attached to its CEEB name. (This causes most
  of the Texas A&M matches to be exact instead incorrect fuzzy matches.)

### Exclusions

There are a lot of CEEB codes that aren't useful for matching with institutions.

- The CEEB data includes many non-educational entities. That being
  politicians, individuals, and companies. These should be excluded since they
  would not have a matching IPEDS or NCES code. There aren't a vast number of
  these so a negative filter is simple enough to isolate most of them. Then flip
  it to a positive filter to remove them. (There was a little over 200 of
  these.)
- Naval and Navy recruiting doesn't need to be included.
- Some institutions offer an 'Upward Bound Program'. I'm electing to exclude
  these since they are an assistance program and not an institution themselves.
- Some institutions also have an 'Educational Talent Search'. These are
  excluded.

## BM25

DuckDB has a macro for this that accepts a single query string.
I tried to wrap this into a macro that accepts a column or somehow make it work directly in DuckDB, but was unsuccessful.
The way around this is to use a loop which isn't able to take advantage of multithreading or vectorization, so it is slower.

Perhaps in the future a version of the FTS extension will allow that functionality.

### Higher Education

I've matched the CEEB names without an exact match onto all the IPEDS names since there are more IPEDS records.
Taking all the IPEDS names allows for CEEB codes with correspond to part of a university to still match their parent university.

The matching was filtered by state so the results are guaranteed to in the correct state.
The city was also indexed alongside the IPEDS name if there isn't a match it is at least more likely to be in the right city.

Between exact matching and conservative fuzzy matching:

```sql
WITH counts AS (
  SELECT 
    ipeds:      count(ipeds) FILTER(ipeds IS NOT NULL AND ipeds_name IS NOT NULL),
    nsc:        count(nsc)   FILTER(nsc   IS NOT NULL AND nsc_name   IS NOT NULL),
    ceeb:       count(ceeb)  FILTER(ceeb  IS NOT NULL AND ceeb_name  IS NOT NULL),
    ceeb_ipeds: count(ceeb)  FILTER(ipeds IS NOT NULL AND ipeds_name IS NOT NULL),
    ceeb_nsc:   count(ceeb)  FILTER(nsc   IS NOT NULL AND nsc_name   IS NOT NULL),
    ipeds_ceeb: count(ipeds) FILTER(ceeb  IS NOT NULL AND ceeb_name  IS NOT NULL),
    ipeds_nsc:  count(ipeds) FILTER(nsc   IS NOT NULL AND nsc_name   IS NOT NULL),
    nsc_ipeds:  count(nsc)   FILTER(ipeds IS NOT NULL AND ipeds_name IS NOT NULL),
    nsc_ceeb:   count(nsc)   FILTER(ceeb  IS NOT NULL AND ceeb_name  IS NOT NULL)
  FROM university_crosswalk
),
percentages AS (
  SELECT 
    ipeds_by_ceeb: round(ceeb_ipeds / ipeds, 4) * 100 || '%',
    ipeds_by_nsc:  round(nsc_ipeds  / ipeds, 4) * 100 || '%',
    ceeb_by_ipeds: round(ipeds_ceeb / ceeb,  4) * 100 || '%',
    ceeb_by_nsc:   round(nsc_ceeb   / ceeb,  4) * 100 || '%',
    nsc_by_ipeds:  round(ipeds_nsc  / nsc,   4) * 100 || '%',
    nsc_by_ceeb:   round(ceeb_nsc   / nsc,   4) * 100 || '%'
  FROM counts
)
UNPIVOT percentages ON COLUMNS(*)
```

| ID Pairing | Coverage | Note |
|:---:|:---:|---|
| IPEDS by CEEB | 32.22% |  |
| IPEDS by NSC  | 74.13% | The IPEDS set is larger than the NSC set. |
| CEEB by IPEDS | 81.19% | CEEB is the smallest set, so when this reaches 100%, that is the best that is possible. |
| CEEB by NSC   | 77.28% |  |
| NSC by IPEDS  | 99.99% | This is high because the NSC table already includes IPEDS codes. |
| NSC by CEEB   | 39.94% |  |

Not all of these are confirmed correct.
There are some known errors.

That is also treating many-to-many connections as okay.

#### Wrong Matches To Be Fixed

These have a high BM25 score, but are definitely wrong.

- University of Wisconsin Fond du Lac
  - `CEEB`: `1942`
  - Merged into University of Wisconsin Oshkosh in 2018 ([Wikipedia](https://en.wikipedia.org/wiki/University_of_Wisconsin%E2%80%93Oshkosh,_Fond_du_Lac_Campus)).
- Keiser Career College Port Saint Lucie
  - `CEEB`: `5355`
  - Renamed to Southeastern College in 2015 ([sec.edu/about](https://www.sec.edu/about/)).
- Keiser University Pembroke Pines
  - `CEEB`: `7760`
  - The relationship between Keiser University and Keiser Career College are
    confusing. Their histories seem to overlap, and yet they exist as two
    different institutions today.

I'm sure there are plenty more.
