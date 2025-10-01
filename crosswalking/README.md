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
- Pulling from the NCAA site takes a very long time. It takes a little under a second to run each query and there over 47,000 queries :neutral_face:.

## Higher Educational Institutions

These have three identification systems: IPEDS, NCS, and CEEB codes.
Converting between IPEDS and NCS is already done via the NCS table from NCS themselves.
Converting between those two and CEEB is more difficult.
It has the same problems as with the high schools, but should not be as difficult since there are fewer institutions.

---

## BM25

DuckDB has a macro for this that accepts a single query string.
I tried to wrap this into a macro that accepts a column or somehow make it work directly in DuckDB, but was unsuccessful.
The way around this is to use a loop which isn't able to take advantage of multithreading or vectorization, so it is slower.

Perhaps in the future a version of the FTS extension will allow that functionality.
