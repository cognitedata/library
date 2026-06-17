WITH base AS (
  SELECT
    cast(key as string) AS external_id,
    cast(name as string) AS name,
    cast(parentExternalId as string) AS parentExternalId,
    cast(description as string) AS description
  FROM `{{ rawDatabase }}`.`isa_asset`
  WHERE is_new('{{ rawDatabase }}', 'isa_asset')
),
withRefs AS (
  SELECT
    external_id,
    name,
    description,
    CASE
      WHEN parentExternalId IS NULL OR parentExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', parentExternalId)
    END AS parent
  FROM base
)
SELECT
  external_id AS externalId,
  FIRST(name, true) AS name,
  FIRST(description, true) AS description,
  FIRST(parent, true) AS parent
FROM withRefs
GROUP BY external_id
