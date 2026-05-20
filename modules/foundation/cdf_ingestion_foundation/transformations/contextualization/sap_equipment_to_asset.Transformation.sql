-- SAP Equipment → ISAAsset relation
-- Sets the Equipment.asset property linking each equipment record to its parent
-- functional location (ISAAsset instance), using the Floc field from the SAP
-- EquipmentListSet OData entity.
-- Verify column name `Floc` against your SAP NW Gateway EquipmentListSet service.

SELECT
  cast(`Equipment` AS STRING)                                         AS externalId,
  node_reference('{{instanceSpace}}', cast(`Floc` AS STRING))         AS asset
FROM `db_{{location}}_sap`.`equipment`
WHERE isnotnull(`Equipment`)
  AND cast(`Equipment` AS STRING) != ''
  AND isnotnull(`Floc`)
  AND cast(`Floc` AS STRING) != ''
