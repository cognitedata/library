WITH base AS (
  SELECT
    cast(key as string) AS external_id,
    cast(key as string) AS siteId,
    cast(name as string) AS siteName,
    cast(parentExternalId as string) AS parentExternalId
  FROM `{{ rawDatabase }}`.`isa_asset`
  WHERE assetSpecific = 'Site'
),
withRefs AS (
  SELECT
    external_id,
    siteId,
    siteName,
    CASE
      WHEN parentExternalId IS NULL OR parentExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', parentExternalId)
    END AS enterprise
  FROM base
)
SELECT
  external_id AS externalId,
  FIRST(siteId, true) AS siteId,
  FIRST(siteName, true) AS siteName,
  FIRST(enterprise, true) AS enterprise
FROM withRefs
GROUP BY external_id
