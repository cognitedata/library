WITH base AS (
  SELECT
    cast(key as string) AS external_id,
    cast(key as string) AS areaId,
    cast(name as string) AS areaName,
    cast(parentExternalId as string) AS parentExternalId
  FROM `{{ rawDatabase }}`.`isa_asset`
  WHERE assetSpecific = 'Area'
),
withRefs AS (
  SELECT
    external_id,
    areaId,
    areaName,
    CASE
      WHEN parentExternalId IS NULL OR parentExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', parentExternalId)
    END AS site
  FROM base
)
SELECT
  external_id AS externalId,
  FIRST(areaId, true) AS areaId,
  FIRST(areaName, true) AS areaName,
  FIRST(site, true) AS site
FROM withRefs
GROUP BY external_id
