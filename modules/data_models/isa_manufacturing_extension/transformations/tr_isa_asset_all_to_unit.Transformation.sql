WITH base AS (
  SELECT
    cast(key as string) AS external_id,
    cast(key as string) AS unitId,
    cast(name as string) AS unitName,
    cast(parentExternalId as string) AS parentExternalId
  FROM `{{ rawDatabase }}`.`isa_asset`
  WHERE assetSpecific = 'Unit'
),
withRefs AS (
  SELECT
    external_id,
    unitId,
    unitName,
    CASE
      WHEN parentExternalId IS NULL OR parentExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', parentExternalId)
    END AS processCell
  FROM base
)
SELECT
  external_id AS externalId,
  FIRST(unitId, true) AS unitId,
  FIRST(unitName, true) AS unitName,
  FIRST(processCell, true) AS processCell
FROM withRefs
GROUP BY external_id
