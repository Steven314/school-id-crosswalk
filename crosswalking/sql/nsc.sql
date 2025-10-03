WITH a AS (
    SELECT 
        nsc: nsc_college_and_branch_code[1:6],
        nsc_branch: nsc_college_and_branch_code[8:9],
        nsc_full: nsc_college_and_branch_code,
        ipeds: ipeds_unit_id::INT::VARCHAR,
        name: nsc_college_name
    FROM nsc.nsc_to_ipeds
)
SELECT DISTINCT 
  arg_min(COLUMNS(*), nsc_full) OVER (PARTITION BY ipeds) 
FROM a
ORDER BY ipeds
