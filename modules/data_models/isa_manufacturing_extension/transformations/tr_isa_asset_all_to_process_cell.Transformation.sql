WITH base AS (
  SELECT
    cast(key as string) AS external_id,
    cast(key as string) AS processCellId,
    cast(name as string) AS processCellName,
    cast(parentExternalId as string) AS parentExternalId
  FROM `{{ rawDatabase }}`.`isa_asset`
  WHERE assetSpecific = 'ProcessCell'
    AND is_new('isa_asset', lastUpdatedTime)
),
withRefs AS (
  SELECT
    external_id,
    processCellId,
    processCellName,
    CASE
      WHEN parentExternalId IS NULL OR parentExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', parentExternalId)
    END AS area
  FROM base
)
SELECT
  external_id AS externalId,
  FIRST(processCellId, true) AS processCellId,
  FIRST(processCellName, true) AS processCellName,
  FIRST(area, true) AS area
FROM withRefs
GROUP BY external_id
