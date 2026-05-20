-- PI TimeSeries → ISATimeSeries
-- Source: CDF TimeSeries created by the PI .NET Extractor (direct-write, no RAW staging).
-- The extractor writes CDM ExtractorTimeSeries instances into {{instanceSpace}} and
-- also registers standard CDF TimeSeries resources tagged to dataset {{piDataset}}.
-- This transformation promotes those TimeSeries into ISATimeSeries instances.

SELECT
  externalId,
  externalId   AS sourceId,
  name,
  description,
  'numeric'    AS type,
  isStep,
  unit         AS sourceUnit,
  'PI'         AS sourceContext,

  -- sysTagsFound: extract equipment tag signals from the PI tag name for
  -- downstream entity-matching and file-annotation contextualization.
  -- The heuristic below extracts the segment before the first '.' or '_'
  -- suffix — adapt the regexp to your site's PI tag naming convention.
  -- Set populateSysTagsFound: false in default.config.yaml to omit this.
  CASE
    WHEN '{{populateSysTagsFound}}' = 'true'
    THEN array(regexp_extract(name, '^([^._]+)', 1))
    ELSE array()
  END          AS sysTagsFound

FROM timeseries()
WHERE dataSetExternalId = '{{piDataset}}'
