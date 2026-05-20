-- SAP Work Orders → WorkOrder
-- Source: RAW table db_{{location}}_sap.workorder written by the SAP OData Extractor
--         from the ExHeaderSet OData entity (ZPM_ORDER_DATA_EXPORT_SRV).
-- Column names are SAP OData property names. Verify against your SAP service definition.

SELECT
  cast(`OrderId`      AS STRING)     AS externalId,
  cast(`OrderId`      AS STRING)     AS sourceId,
  cast(`ShortText`    AS STRING)     AS name,
  cast(`ShortText`    AS STRING)     AS description,
  cast(`SystemStatus` AS STRING)     AS status,
  cast(`OrderType`    AS STRING)     AS type
FROM `db_{{location}}_sap`.`workorder`
WHERE isnotnull(`OrderId`)
  AND cast(`OrderId` AS STRING) != ''
