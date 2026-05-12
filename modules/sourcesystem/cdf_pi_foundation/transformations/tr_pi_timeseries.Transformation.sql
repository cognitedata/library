-- Transformation: PI tag metadata → ISATimeSeries DM instances
-- Target view : ISATimeSeries (space: {{schemaSpace}}, version: {{dataModelVersion}})
-- Source RAW  : db_{{location}}_pi.timeseries_metadata
--
-- SCAFFOLD — column names (name, description, unit, etc.) reflect the default PI .NET Extractor
-- RAW schema. Verify against your actual RAW table before running in production.
-- See .cursor/rules/cdf-transformations.mdc for AI-assisted adaptation guidance.

SELECT
  -- Identity
  concat('{{piIdPrefix}}', name)    AS externalId,
  '{{instanceSpace}}'               AS space,

  -- Core metadata
  name                              AS name,
  description                       AS description,
  'numeric'                         AS type,
  false                             AS isStep,

  -- Unit — try to resolve to CDF unit catalogue node; fall back to raw string
  if(
    try_get_unit(unit) IS NOT NULL,
    node_reference('cdf_cdm_units', try_get_unit(unit)),
    NULL
  )                                 AS unit,
  unit                              AS sourceUnit,

  -- Source context label
  'PI'                              AS sourceContext,

  -- sysTagsFound: array of candidate tag identifiers for downstream SQL contextualization.
  -- The pattern below extracts the segment before the first ':' or '_' delimiter,
  -- which often corresponds to a functional location or equipment tag in PI naming conventions.
  -- Adapt the regexp to match your site's PI tag naming convention.
  -- Set populateSysTagsFound: false in default.config.yaml to omit this field entirely.
  case
    when '{{populateSysTagsFound}}' = 'true'
    then array(
      regexp_extract(
        regexp_replace(name, '^[A-Z]+_', ''),  -- strip common prefixes (VAL_, AL_, etc.)
        '^[^:_.]+',                             -- take first segment before ':', '_', or '.'
        0
      )
    )
    else array()
  end                               AS sysTagsFound

FROM `db_{{location}}_pi`.`timeseries_metadata`
