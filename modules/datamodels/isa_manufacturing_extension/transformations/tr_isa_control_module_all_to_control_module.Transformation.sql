WITH base AS (
  SELECT
    cast(key as string) AS external_id,
    cast(controlModuleId as string) AS controlModuleId,
    cast(controlModuleName as string) AS name,
    cast(description as string) AS description,
    cast(controlType as string) AS controlType,
    cast(controllerAddress as string) AS controllerAddress,
    cast(equipmentModuleExternalId as string) AS equipmentModuleExternalId
  FROM `{{ rawDatabase }}`.`isa_control_module`
  WHERE 1=1 -- full reload: is_new('isa_control_module', lastUpdatedTime)
),
withRefs AS (
  SELECT
    external_id,
    controlModuleId,
    name,
    description,
    controlType,
    controllerAddress,
    CASE
      WHEN equipmentModuleExternalId IS NULL OR equipmentModuleExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', equipmentModuleExternalId)
    END AS equipmentModule
  FROM base
)
SELECT
  external_id AS externalId,
  FIRST(controlModuleId, true) AS controlModuleId,
  FIRST(name, true) AS name,
  FIRST(description, true) AS description,
  FIRST(controlType, true) AS controlType,
  FIRST(controllerAddress, true) AS controllerAddress,
  FIRST(equipmentModule, true) AS equipmentModule
FROM withRefs
GROUP BY external_id
