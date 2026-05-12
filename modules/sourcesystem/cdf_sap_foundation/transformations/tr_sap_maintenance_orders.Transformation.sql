-- Transformation: SAP Work Orders → WorkOrder DM instances
-- Target view : WorkOrder (space: {{schemaSpace}}, version: {{dataModelVersion}})
-- Source RAW  : db_{{location}}_sap.workorder
-- Run order   : 4th — run after tr_sap_assets
--
-- SCAFFOLD — SAP column names (OrderId, Descript, OrderType, etc.) reflect the default
-- ExHeaderSet OData entity schema. Verify against your actual RAW table.
-- See .cursor/rules/cdf-transformations.mdc for AI-assisted adaptation guidance.

SELECT
  cast(OrderId AS STRING)                                          AS externalId,
  '{{instanceSpace}}'                                              AS space,

  cast(ShortText AS STRING)                                        AS name,
  cast(OrderId AS STRING)                                          AS sourceId,
  cast(OrderType AS STRING)                                        AS type,
  cast(OrderStatus AS STRING)                                      AS status,
  cast(BasicStartDate AS TIMESTAMP)                                AS scheduledStartTime,
  cast(BasicFinDate AS TIMESTAMP)                                  AS scheduledEndTime,
  cast(ActualStart AS TIMESTAMP)                                   AS startTime,
  cast(ActualFinish AS TIMESTAMP)                                  AS endTime,
  'SAP Work Order'                                                 AS sourceContext,

  -- sysTagsFound: functional location tag for downstream SQL contextualization
  -- Set populateSysTagsFound: false in default.config.yaml to omit
  case
    when '{{populateSysTagsFound}}' = 'true' AND isnotnull(FunctLoc)
    then array(cast(FunctLoc AS STRING))
    else array()
  end                                                              AS sysTagsFound

FROM `db_{{location}}_sap`.`workorder`
WHERE isnotnull(OrderId)
