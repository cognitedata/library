WITH base AS (
  SELECT
    cast(key as string) AS external_id,
    cast(controlModuleName as string) AS name,
    cast(description as string) AS description,
    cast(equipmentModuleExternalId as string) AS equipmentModuleExternalId
  FROM `{{ rawDatabase }}`.`isa_control_module`
  WHERE 1=1 -- full reload: is_new('isa_control_module', lastUpdatedTime)
),
withRefs AS (
  SELECT
    external_id,
    name,
    description,
    CASE
      WHEN equipmentModuleExternalId IS NULL OR equipmentModuleExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', equipmentModuleExternalId)
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
