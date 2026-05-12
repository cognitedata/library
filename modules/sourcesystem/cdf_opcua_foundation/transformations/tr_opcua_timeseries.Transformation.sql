-- Transformation: OPC-UA node metadata → ISATimeSeries DM instances
-- Target view : ISATimeSeries (space: {{schemaSpace}}, version: {{dataModelVersion}})
-- Source RAW  : db_{{location}}_opcua.nodes
--
-- SCAFFOLD — column names reflect the default OPC-UA Extractor RAW schema when
-- store-raw-metadata: true is set. OPC-UA node structure is highly site-specific;
-- verify column names against your actual RAW table before running in production.
-- See .cursor/rules/cdf-transformations.mdc for AI-assisted adaptation guidance.
--
-- Default OPC-UA Extractor RAW columns (verify for your extractor version):
--   Id          — OPC-UA node ID string (e.g. "ns=2;s=Temperature.Tag1")
--   DisplayName — Human-readable node name
--   Description — Node description (may be empty)
--   DataType    — OPC-UA data type (e.g. "Double", "Float", "Int32")
--   NodeClass   — "Variable" for measurement nodes

SELECT
  -- Identity: use the extractor-assigned externalId (id-prefix + node id)
  concat('{{opcuaIdPrefix}}', Id)     AS externalId,
  '{{instanceSpace}}'                 AS space,

  -- Core metadata
  DisplayName                         AS name,
  Description                         AS description,

  -- Map OPC-UA DataType to CDF timeseries type
  case
    when lower(DataType) in ('boolean')         then 'string'
    when lower(DataType) in ('string', 'localizedtext') then 'string'
    else 'numeric'
  end                                 AS type,
  false                               AS isStep,

  -- Source context label
  'OPC-UA'                            AS sourceContext,

  -- sysTagsFound: array of candidate tag identifiers for downstream SQL contextualization.
  -- The pattern below extracts the last segment of the OPC-UA display name, which often
  -- corresponds to a functional location or equipment tag. Adapt to your site's node naming.
  -- Set populateSysTagsFound: false in default.config.yaml to omit this field entirely.
  case
    when '{{populateSysTagsFound}}' = 'true'
    then array(
      regexp_extract(DisplayName, '[^._\\-]+$', 0)  -- last segment after '.', '_', or '-'
    )
    else array()
  end                                 AS sysTagsFound

FROM `db_{{location}}_opcua`.`nodes`
WHERE NodeClass = 'Variable'   -- only ingest measurement nodes, not object/folder nodes
