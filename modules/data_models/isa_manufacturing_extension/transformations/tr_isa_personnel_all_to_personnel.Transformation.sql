WITH base AS (
  SELECT
    cast(key as string) AS external_id,
    cast(personnelId as string) AS personnelId,
    cast(name as string) AS name,
    cast(role as string) AS role,
    cast(siteExternalId as string) AS siteExternalId
  FROM `{{ rawDatabase }}`.`isa_personnel`
  WHERE is_new('{{ rawDatabase }}', 'isa_personnel')
),
withRefs AS (
  SELECT
    external_id,
    personnelId,
    name,
    role,
    CASE
      WHEN siteExternalId IS NULL OR siteExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', siteExternalId)
    END AS site
  FROM base
)
SELECT
  external_id AS externalId,
  FIRST(personnelId, true) AS personnelId,
  FIRST(name, true) AS name,
  FIRST(role, true) AS role,
  FIRST(site, true) AS site
FROM withRefs
GROUP BY external_id
