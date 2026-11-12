WITH instruments AS (
  SELECT
      cast(`key` AS STRING)                              AS instrument_key
    , nullif(trim(cast(name AS STRING)), '')             AS name
    , cast(nullif(trim(cast(setPointHigh AS STRING)), '') AS DOUBLE) AS limit_high
    , cast(nullif(trim(cast(setPointLow AS STRING)), '') AS DOUBLE)  AS limit_low
  FROM `{{ raw_database_sap }}`.`instrument_equipment`
)

, first_time_series AS (
  SELECT
      nullif(trim(cast(asset_externalId AS STRING)), '') AS asset_key
    , min(cast(`key` AS STRING))                         AS time_series_key
  FROM `{{ raw_database_pi }}`.`timeseries`
  WHERE nullif(trim(cast(asset_externalId AS STRING)), '') IS NOT NULL
  GROUP BY nullif(trim(cast(asset_externalId AS STRING)), '')
)

SELECT
    concat(i.instrument_key, '_limit')                   AS externalId
  , i.name                                               AS name
  , i.limit_high                                         AS limitHigh
  , i.limit_low                                          AS limitLow
  , CASE
      WHEN t.time_series_key IS NULL THEN NULL
      ELSE node_reference('{{ instance_space }}', t.time_series_key)
    END                                                  AS timeSeries
FROM instruments AS i
LEFT JOIN first_time_series AS t
  ON i.instrument_key = t.asset_key
WHERE i.instrument_key IS NOT NULL
  AND (i.limit_high IS NOT NULL OR i.limit_low IS NOT NULL)
