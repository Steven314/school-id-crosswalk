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
