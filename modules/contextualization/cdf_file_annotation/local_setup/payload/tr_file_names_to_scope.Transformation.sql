-- Test helper: derive primary (site) and secondary (unit) scope from file names.
-- Examples:
--   VAL_23-PID-001.pdf  -> site=VAL, unit=23
--   23-PID-001.pdf      -> site=null, unit=23
-- Expects a single fileInstanceSpace string (not empty / multi-space).
SELECT
    cast(f.externalId AS STRING) AS externalId
  , CASE
      WHEN f.name RLIKE '^[A-Za-z]{2,}[_.:-]'
      THEN regexp_extract(f.name, '^([A-Za-z]{2,})[_.:-]', 1)
      ELSE NULL
    END AS site
  , nullif(regexp_extract(f.name, '([0-9]{2})', 1), '') AS unit
FROM cdf_nodes('cdf_cdm', 'CogniteFile', 'v1') f
WHERE f.space = '{{ fileInstanceSpace }}'
  AND f.name IS NOT NULL
  AND f.name != ''
