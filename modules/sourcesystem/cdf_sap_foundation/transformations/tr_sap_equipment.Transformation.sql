-- Transformation: SAP Equipment → Equipment DM instances
-- Target view : Equipment (space: {{schemaSpace}}, version: {{dataModelVersion}})
-- Source RAW  : db_{{location}}_sap.equipment
-- Run order   : 2nd — run after tr_sap_assets
--
-- SCAFFOLD — SAP column names (Equipment, Descript, Eqtyp, Manfactur, etc.) reflect the
-- default EquipmentListSet OData entity schema. Verify against your actual RAW table.
-- See .cursor/rules/cdf-transformations.mdc for AI-assisted adaptation guidance.

SELECT
  concat('{{sapSystem}}:equip:', cast(Equipment AS STRING))    AS externalId,
  '{{instanceSpace}}'                                          AS space,

  cast(Descript AS STRING)                                     AS name,
  cast(Equipment AS STRING)                                    AS sourceId,
  cast(Eqtyp AS STRING)                                        AS type,          -- equipment category
  cast(Manfactur AS STRING)                                    AS manufacturer,
  cast(Maintplant AS STRING)                                   AS sourceContext,
  'SAP Equipment'                                              AS sourceContext

FROM `db_{{location}}_sap`.`equipment`
WHERE isnotnull(Equipment)
