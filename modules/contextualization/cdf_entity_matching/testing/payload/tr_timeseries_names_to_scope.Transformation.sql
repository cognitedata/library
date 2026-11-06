-- Test helper: derive primary (site) and secondary (unit) scope from time series names.
-- Examples:
--   VAL_23-KA-9101:X.Value  -> site=VAL, unit=23
--   23-KA-9101:X.Value      -> site=null, unit=23
-- Destination instanceSpace must be a single space; reuse assetInstanceSpace (not a list).
SELECT
    cast(t.externalId AS STRING) AS externalId
  , CASE
      WHEN t.name RLIKE '^[A-Za-z]{2,}[_.:-]'
      THEN regexp_extract(t.name, '^([A-Za-z]{2,})[_.:-]', 1)
      ELSE NULL
    END AS site
  , nullif(regexp_extract(t.name, '([0-9]{2})', 1), '') AS unit
FROM cdf_nodes('cdf_cdm', 'CogniteTimeSeries', 'v1') t
WHERE t.space = '{{ assetInstanceSpace }}'
  AND t.name IS NOT NULL
  AND t.name != ''
