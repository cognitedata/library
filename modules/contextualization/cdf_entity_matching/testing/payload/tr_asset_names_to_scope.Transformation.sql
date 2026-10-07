-- Test helper: derive primary (site) and secondary (unit) scope from asset names.
-- Examples:
--   VAL_23-KA-9101  -> site=VAL, unit=23
--   23-KA-9101      -> site=null, unit=23
-- Expects a single assetInstanceSpace string (not a list).
SELECT
    cast(a.externalId AS STRING) AS externalId
  , CASE
      WHEN a.name RLIKE '^[A-Za-z]{2,}[_.:-]'
      THEN regexp_extract(a.name, '^([A-Za-z]{2,})[_.:-]', 1)
      ELSE NULL
    END AS site
  , nullif(regexp_extract(a.name, '([0-9]{2})', 1), '') AS unit
FROM cdf_nodes('cdf_cdm', 'CogniteAsset', 'v1') a
WHERE a.space = '{{ assetInstanceSpace }}'
  AND a.name IS NOT NULL
  AND a.name != ''
