-- SAP Notifications → Operation
-- Source: RAW table db_{{location}}_sap.workitem written by the SAP OData Extractor
--         from the ExNotifheader OData entity (ZPM_NOTI_EXTRACT_DATA_SRV, filtered by {{sapPlant}}).
-- Column names are SAP OData property names. Verify against your SAP service definition.

-- Deduplicate: extractor may write duplicate rows for the same NotifNo
WITH unique_notifications AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY `NotifNo` ORDER BY `NotifNo`) AS rn
  FROM `db_{{location}}_sap`.`workitem`
)
SELECT
  cast(`NotifNo`   AS STRING)  AS externalId,
  cast(`NotifNo`   AS STRING)  AS sourceId,
  cast(`ShortText` AS STRING)  AS name,
  cast(`ShortText` AS STRING)  AS description,
  cast(`NotifType` AS STRING)  AS type
FROM unique_notifications
WHERE isnotnull(`NotifNo`)
  AND cast(`NotifNo` AS STRING) != ''
  AND rn = 1
