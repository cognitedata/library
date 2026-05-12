-- Transformation: SAP Work Tasks/Operations → Operation DM instances
-- Target view : Operation (space: {{schemaSpace}}, version: {{dataModelVersion}})
-- Source RAW  : db_{{location}}_sap.worktask
-- Run order   : 5th — run after tr_sap_maintenance_orders
--
-- SCAFFOLD — SAP column names (OrderId, Activity, LongText, etc.) reflect the default
-- ExOperationsSet OData entity schema. Verify against your actual RAW table.
-- See .cursor/rules/cdf-transformations.mdc for AI-assisted adaptation guidance.

-- Deduplicate on composite key (OrderId + Activity) in case the extractor produces duplicates
WITH unique_operations AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY OrderId, Activity
      ORDER BY OrderId
    ) AS rn
  FROM `db_{{location}}_sap`.`worktask`
)
SELECT
  concat(cast(OrderId AS STRING), '-', cast(Activity AS STRING))  AS externalId,
  '{{instanceSpace}}'                                              AS space,

  cast(LongText AS STRING)                                         AS name,
  concat(cast(OrderId AS STRING), '-', cast(Activity AS STRING))  AS sourceId,
  cast(ControlKey AS STRING)                                       AS type,
  cast(UserStatus AS STRING)                                       AS status,
  cast(EarlySchedStart AS TIMESTAMP)                               AS scheduledStartTime,
  cast(EarlySchedFin AS TIMESTAMP)                                 AS scheduledEndTime,
  'SAP Operation'                                                  AS sourceContext

FROM unique_operations
WHERE isnotnull(OrderId)
  AND isnotnull(Activity)
  AND rn = 1
