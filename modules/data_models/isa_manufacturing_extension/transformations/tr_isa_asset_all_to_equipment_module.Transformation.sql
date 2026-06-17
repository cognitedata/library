WITH base AS (
  SELECT
    cast(key as string) AS external_id,
    cast(key as string) AS equipmentModuleId,
    cast(name as string) AS equipmentModuleName,
    cast(parentExternalId as string) AS parentExternalId
  FROM `{{ rawDatabase }}`.`isa_asset`
  WHERE assetSpecific = 'EquipmentModule'
    AND is_new('{{ rawDatabase }}', 'isa_asset')
),
withRefs AS (
  SELECT
    external_id,
    equipmentModuleId,
    equipmentModuleName,
    CASE
      WHEN parentExternalId IS NULL OR parentExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', parentExternalId)
    END AS unit
  FROM base
)
SELECT
  external_id AS externalId,
  FIRST(equipmentModuleId, true) AS equipmentModuleId,
  FIRST(equipmentModuleName, true) AS equipmentModuleName,
  FIRST(unit, true) AS unit
FROM withRefs
GROUP BY external_id
