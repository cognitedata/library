-- OPC-UA TimeSeries → ISATimeSeries
-- Source: RAW table written by the OPC-UA Extractor metadata-targets config.
-- The extractor writes node browse metadata to db_{{location}}_opcua.timeseries
-- (controlled by metadata-targets.raw.timeseries-table in the extractor config).
-- Column names match the OPC-UA Extractor default RAW schema.
-- Verify field names against your extractor version if columns are missing.

SELECT
  cast(`externalId`    AS STRING)  AS externalId,
  cast(`externalId`    AS STRING)  AS sourceId,
  cast(`name`          AS STRING)  AS name,
  cast(`description`   AS STRING)  AS description,
  CASE
    WHEN lower(cast(`dataType` AS STRING)) IN ('boolean', 'integer', 'int16', 'int32', 'int64', 'uint16', 'uint32', 'uint64')
    THEN true
    ELSE false
  END                              AS isStep,
  'OPC-UA'                         AS sourceContext
FROM `db_{{location}}_opcua`.`timeseries`
WHERE isnotnull(`externalId`)
