WITH changedEquipmentIds AS (
  SELECT CAST(`key` AS STRING) AS external_id
  FROM `{{ rawDatabase }}`.`isa_equipment`
  WHERE is_new('isa_equipment', lastUpdatedTime)
  UNION
  SELECT CAST(equipmentExternalId AS STRING) AS external_id
  FROM `{{ rawDatabase }}`.`isa_file`
  WHERE equipmentExternalId IS NOT NULL AND equipmentExternalId <> ''
    AND is_new('isa_file', lastUpdatedTime)
),
src AS (
  SELECT
    CAST(e.`key` AS STRING) AS external_id,
    CAST(e.equipmentId AS STRING) AS equipmentId,
    CAST(e.equipmentName AS STRING) AS name,
    CAST(e.description AS STRING) AS description,
    CAST(e.unitExternalId AS STRING) AS unitExternalId,
    CAST(e.manufacturer AS STRING) AS manufacturer,
    CAST(e.model AS STRING) AS model,
    CAST(e.serialNumber AS STRING) AS serialNumber,
    CAST(e.assetExternalId AS STRING) AS assetExternalId
  FROM `{{ rawDatabase }}`.`isa_equipment` e
  INNER JOIN changedEquipmentIds c ON CAST(e.`key` AS STRING) = c.external_id
),
withRefs AS (
  SELECT
    external_id,
    equipmentId,
    name,
    description,
    manufacturer,
    model,
    serialNumber,
    CASE
      WHEN unitExternalId IS NULL OR unitExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', unitExternalId)
    END AS unit,
    CASE
      WHEN assetExternalId IS NULL OR assetExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', assetExternalId)
    END AS asset
  FROM src
),
files AS (
  SELECT
    CAST(equipmentExternalId AS STRING) AS external_id,
    COLLECT_SET(node_reference('{{ instance_space }}', CONCAT('isa_manufacturing_file_', `key`))) AS files
  FROM `{{ rawDatabase }}`.`isa_file`
  WHERE equipmentExternalId IS NOT NULL AND equipmentExternalId <> ''
  GROUP BY equipmentExternalId
)
SELECT
  withRefs.external_id AS externalId,
  FIRST(withRefs.equipmentId, true) AS equipmentId,
  FIRST(withRefs.name, true) AS name,
  FIRST(withRefs.description, true) AS description,
  FIRST(withRefs.unit, true) AS unit,
  FIRST(withRefs.manufacturer, true) AS manufacturer,
  FIRST(withRefs.model, true) AS model,
  FIRST(withRefs.serialNumber, true) AS serialNumber,
  FIRST(withRefs.asset, true) AS asset,
  array_distinct(flatten(collectList(files.files))) AS files
FROM withRefs
LEFT JOIN files ON withRefs.external_id = files.external_id
GROUP BY withRefs.external_id
