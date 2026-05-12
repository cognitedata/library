-- Transformation: SAP Functional Locations → ISAAsset DM instances
-- Target view : ISAAsset (space: {{schemaSpace}}, version: {{dataModelVersion}})
-- Source RAW  : db_{{location}}_sap.functional_location
-- Run order   : 1st — must complete before equipment and work order transformations
--
-- SCAFFOLD — SAP column names (Functlocation, Descript, Fltyp, etc.) reflect the default
-- FunclocListSet OData entity schema. Column names vary across SAP versions and NW Gateway
-- configurations. Verify against your actual RAW table before running in production.
-- See .cursor/rules/cdf-transformations.mdc for AI-assisted adaptation guidance.

WITH parent_lookup AS (
  -- Resolve parent functional location for hierarchy building
  -- SAP stores the superior functional location in a dedicated column
  SELECT
    concat('{{sapSystem}}:floc:', cast(child.Functlocation AS STRING))    AS externalId,
    node_reference(
      '{{instanceSpace}}',
      concat('{{sapSystem}}:floc:', cast(parent.Functlocation AS STRING))
    )                                                                       AS parent
  FROM `db_{{location}}_sap`.`functional_location` AS child
  JOIN `db_{{location}}_sap`.`functional_location` AS parent
    ON child.Supfloc = parent.Functlocation          -- Supfloc: superior functional location field
  WHERE isnotnull(child.Functlocation)
    AND isnotnull(parent.Functlocation)
)
SELECT
  concat('{{sapSystem}}:floc:', cast(f.Functlocation AS STRING))  AS externalId,
  '{{instanceSpace}}'                                              AS space,

  cast(f.Descript AS STRING)                                       AS name,
  cast(f.Functlocation AS STRING)                                  AS sourceId,
  cast(f.Fltyp AS STRING)                                          AS type,       -- functional location category
  'SAP Functional Location'                                        AS sourceContext,

  parent_lookup.parent                                             AS parent

FROM `db_{{location}}_sap`.`functional_location` AS f
LEFT JOIN parent_lookup
  ON concat('{{sapSystem}}:floc:', cast(f.Functlocation AS STRING)) = parent_lookup.externalId
WHERE isnotnull(f.Functlocation)
