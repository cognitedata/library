WITH equipment_lookup AS (
  SELECT
      nullif(trim(cast(asset_externalId AS STRING)), '')                      AS asset_key
    , collect_set(node_reference('{{ instance_space }}', cast(`key` AS STRING))) AS equipment
  FROM `{{ raw_database_sap }}`.`equipment`
  WHERE nullif(trim(cast(asset_externalId AS STRING)), '') IS NOT NULL
  GROUP BY nullif(trim(cast(asset_externalId AS STRING)), '')
)

, source AS (
  SELECT
      cast(`key` AS STRING)                              AS time_series_key
    , nullif(trim(cast(name AS STRING)), '')             AS name
    , nullif(trim(cast(description AS STRING)), '')      AS description
    , upper(trim(cast(timeSeriesType AS STRING)))        AS time_series_type
    , lower(trim(cast(isString AS STRING)))              AS is_string
    , lower(trim(cast(isStep AS STRING)))                AS is_step
    , nullif(trim(cast(pi_tag AS STRING)), '')           AS historian_tag_name
    , nullif(trim(cast(pi_pointSource AS STRING)), '')   AS historian_point_source
    , nullif(trim(cast(engunits AS STRING)), '')         AS source_unit
    , nullif(trim(cast(asset_externalId AS STRING)), '') AS asset_key
    , nullif(trim(cast(sourceId AS STRING)), '')         AS source_id
    , nullif(trim(cast(source AS STRING)), '')           AS source_context
  FROM `{{ raw_database_pi }}`.`timeseries`
)

SELECT
    s.time_series_key                                    AS externalId
  , s.name                                               AS name
  , s.description                                        AS description
  , CASE WHEN s.is_string = 'true' THEN 'string' ELSE 'numeric' END AS `type`
  , s.is_step = 'true'                                   AS isStep
  , CASE WHEN s.time_series_type = 'PI' THEN 'Historian' ELSE 'Other' END AS timeSeriesSource
  , s.historian_tag_name                                 AS historianTagName
  , s.historian_point_source                             AS historianPointSource
  , s.source_unit                                        AS sourceUnit
  , CASE
      WHEN s.asset_key IS NULL THEN NULL
      ELSE array(node_reference('{{ instance_space }}', s.asset_key))
    END                                                  AS assets
  , e.equipment                                          AS equipment
  , s.source_id                                          AS sourceId
  , s.source_context                                     AS sourceContext
FROM source AS s
LEFT JOIN equipment_lookup AS e
  ON s.asset_key = e.asset_key
WHERE s.time_series_key IS NOT NULL
