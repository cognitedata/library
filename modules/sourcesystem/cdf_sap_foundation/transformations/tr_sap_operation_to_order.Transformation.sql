-- Transformation: Operation → WorkOrder relation (sets workOrder property on Operation instances)
-- Target view : Operation (space: {{schemaSpace}}, version: {{dataModelVersion}})
-- Source RAW  : db_{{location}}_sap.worktask
-- Run order   : 6th — run after both tr_sap_maintenance_orders and tr_sap_operations
--
-- SCAFFOLD — joins worktask to workorder via OrderId to resolve the parent work order reference.
-- Verify column names against your actual RAW tables.
-- See .cursor/rules/cdf-transformations.mdc for AI-assisted adaptation guidance.

WITH unique_operations AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY OrderId, Activity ORDER BY OrderId) AS rn
  FROM `db_{{location}}_sap`.`worktask`
),
unique_orders AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY OrderId ORDER BY OrderId) AS rn
  FROM `db_{{location}}_sap`.`workorder`
)
SELECT
  concat(cast(op.OrderId AS STRING), '-', cast(op.Activity AS STRING))  AS externalId,
  '{{instanceSpace}}'                                                    AS space,

  -- workOrder: direct relation to the parent WorkOrder instance
  node_reference(
    '{{instanceSpace}}',
    cast(wo.OrderId AS STRING)
  )                                                                       AS workOrder

FROM unique_operations AS op
JOIN unique_orders AS wo
  ON op.OrderId = wo.OrderId
WHERE isnotnull(op.OrderId)
  AND isnotnull(op.Activity)
  AND isnotnull(wo.OrderId)
  AND op.rn = 1
  AND wo.rn = 1
