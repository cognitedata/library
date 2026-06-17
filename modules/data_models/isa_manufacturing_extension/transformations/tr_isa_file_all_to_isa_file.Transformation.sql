WITH base AS (
  SELECT
    concat('isa_manufacturing_file_', key) AS external_id,
    cast(name AS string) AS name,
    cast(directory AS string) AS directory,
    cast(mimeType AS string) AS mimeType,
    cast(assetExternalId AS string) AS assetExternalId
  FROM `{{ rawDatabase }}`.`isa_file`
  WHERE is_new('isa_file', lastUpdatedTime)
),
withRefs AS (
  SELECT
    external_id,
    name,
    directory,
    mimeType,
    CASE
      WHEN assetExternalId IS NULL OR trim(assetExternalId) = '' THEN NULL
      ELSE array(node_reference('{{ instance_space }}', trim(assetExternalId)))
    END AS assets
  FROM base
)
SELECT
  external_id AS externalId,
  FIRST(name, true) AS name,
  FIRST(directory, true) AS directory,
  FIRST(mimeType, true) AS mimeType,
  array_distinct(flatten(collectList(assets))) AS assets
FROM withRefs
GROUP BY external_id
