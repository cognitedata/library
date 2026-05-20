-- SAP Equipment → Equipment
-- Source: RAW table db_{{location}}_sap.equipment written by the SAP OData Extractor
--         from the EquipmentListSet OData entity (filtered to plant {{sapPlant}}).
-- Column names are SAP OData property names from ZGW_GETEQIP_SRV.
-- Verify field names against your SAP NW Gateway service definition.

SELECT
  cast(`Equipment` AS STRING)  AS externalId,
  cast(`Equipment` AS STRING)  AS sourceId,
  cast(`Descript`  AS STRING)  AS name,
  cast(`Descript`  AS STRING)  AS description
FROM `db_{{location}}_sap`.`equipment`
WHERE isnotnull(`Equipment`)
  AND cast(`Equipment` AS STRING) != ''
