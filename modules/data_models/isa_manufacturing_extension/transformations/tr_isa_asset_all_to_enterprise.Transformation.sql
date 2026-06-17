WITH base AS (
  SELECT
    cast(key as string) AS external_id,
    cast(key as string) AS enterpriseId,
    cast(name as string) AS enterpriseName
  FROM `{{ rawDatabase }}`.`isa_asset`
  WHERE assetSpecific = 'Enterprise'
    AND is_new('isa_asset', lastUpdatedTime)
)
SELECT
  external_id AS externalId,
  FIRST(enterpriseId, true) AS enterpriseId,
  FIRST(enterpriseName, true) AS enterpriseName
FROM base
GROUP BY external_id
