WITH changedEquipmentModuleIds AS (
  SELECT CAST(`key` AS STRING) AS equipmentModuleId
  FROM `{{ rawDatabase }}`.`isa_asset`
  WHERE assetSpecific = 'EquipmentModule'
    AND is_new('{{ rawDatabase }}', 'isa_asset')
  UNION
  SELECT CAST(assetExternalId AS STRING) AS equipmentModuleId
  FROM `{{ rawDatabase }}`.`isa_equipment`
  WHERE assetExternalId IS NOT NULL AND assetExternalId <> ''
    AND is_new('{{ rawDatabase }}', 'isa_equipment')
),
equipMod AS (
  SELECT
    CAST(a.`key` AS STRING) AS equipModExtId,
    CAST(a.`key` AS STRING) AS equipModId,
    CAST(a.name AS STRING) AS equipModName
  FROM `{{ rawDatabase }}`.`isa_asset` a
  INNER JOIN changedEquipmentModuleIds c ON CAST(a.`key` AS STRING) = c.equipmentModuleId
  WHERE a.assetSpecific = 'EquipmentModule'
),
equip AS (
  SELECT
    CAST(`key` AS STRING) AS equipExtId,
    CAST(assetExternalId AS STRING) AS assetExternalId
  FROM `{{ rawDatabase }}`.`isa_equipment`
  WHERE assetExternalId IS NOT NULL AND assetExternalId <> ''
)
SELECT
  equipMod.equipModExtId AS externalId,
  equipMod.equipModId AS equipmentModuleId,
  equipMod.equipModName AS equipmentModuleName,
  COLLECT_SET(node_reference('{{ instance_space }}', equip.equipExtId)) AS equipment
FROM equipMod
LEFT JOIN equip ON equipMod.equipModExtId = equip.assetExternalId
GROUP BY
  equipMod.equipModExtId,
  equipMod.equipModId,
  equipMod.equipModName
