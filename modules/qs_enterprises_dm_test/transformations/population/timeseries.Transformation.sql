-- =============================================================================
-- TIMESERIES POPULATION TRANSFORMATION
-- =============================================================================
-- Loads PI time series metadata from RAW into the enterprise CogniteTimeSeries
-- data model. Source data is ingested by the PI extractor into
-- {{ rawSourceDatabase }}.timeseries_metadata
-- =============================================================================

select
  -- Identity
  externalId                                          as externalId,
  externalId                                          as sourceId,
  name                                                as name,
  description                                         as description,

  -- TimeSeries core properties
  'numeric'                                           as type,
  false                                               as isStep,

  -- Unit mapping: resolve to CDM unit node if possible
  if(
    try_get_unit(`unit`) IS NOT NULL,
    node_reference('cdf_cdm_units', try_get_unit(`unit`)),
    NULL
  )                                                   as unit,
  `unit`                                              as sourceUnit,

  -- Source context
  'Time Series'                                       as sourceContext,
  -- sysTagsFound extraction from PI tag naming convention
  -- Pattern: strips prefixes (VAL_, AL_, etc.) and extracts the asset tag
  case
    when name like '%-%' and name like '%:%' then
      array(
        regexp_extract(
          regexp_replace(name, '^[A-Z]+_', ''),
          '^[^:_]+',
          0
        )
      )
    else array()
  end                                                 as sysTagsFound

from `{{ rawSourceDatabase }}`.`timeseries_metadata`
