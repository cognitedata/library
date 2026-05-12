-- Transformation: Equipment → Asset edge (sets asset property on Equipment instances)
-- Target view : Equipment (space: {{schemaSpace}}, version: {{dataModelVersion}})
-- Source RAW  : db_{{location}}_sap.equipment joined with functional_location
-- Run order   : 3rd — run after both tr_sap_assets and tr_sap_equipment
--
-- SCAFFOLD — links each equipment to its functional location (parent asset) via the
-- Funcloc column (functional location assigned to the equipment in SAP).
-- Verify column name against your actual RAW schema.
-- See .cursor/rules/cdf-transformations.mdc for AI-assisted adaptation guidance.

SELECT
  concat('{{sapSystem}}:equip:', cast(e.Equipment AS STRING))     AS externalId,
  '{{instanceSpace}}'                                             AS space,

  -- asset: direct relation to the ISAAsset instance for the equipment's functional location
  node_reference(
    '{{instanceSpace}}',
    concat('{{sapSystem}}:floc:', cast(e.Funcloc AS STRING))      -- Funcloc: assigned functional location
  )                                                               AS asset

FROM `db_{{location}}_sap`.`equipment` AS e
WHERE isnotnull(e.Equipment)
  AND isnotnull(e.Funcloc)
