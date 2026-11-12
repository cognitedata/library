WITH source AS (
  SELECT
      cast(`key` AS STRING)                                                 AS notification_key
    , nullif(trim(cast(name AS STRING)), '')                                AS name
    , nullif(trim(cast(description AS STRING)), '')                         AS description
    , nullif(trim(cast(systemStatusCode AS STRING)), '')                    AS status
    , nullif(regexp_extract(trim(cast(notificationPriorityType AS STRING)), '^([0-9]+)', 1), '') AS priority_number
    , nullif(trim(cast(failureModeCode AS STRING)), '')                     AS failure_mode_code
    , nullif(trim(cast(detectionMethodCode AS STRING)), '')                 AS detection_method_code
    , nullif(trim(cast(asset_externalId AS STRING)), '')                    AS asset_key
    , nullif(trim(cast(maintenanceOrder_externalId AS STRING)), '')         AS maintenance_order_key
    , nullif(trim(cast(startTime AS STRING)), '')                           AS start_time
    , nullif(trim(cast(endTime AS STRING)), '')                             AS end_time
    , nullif(trim(cast(sourceId AS STRING)), '')                            AS source_id
    , nullif(trim(cast(source AS STRING)), '')                              AS source_context
  FROM `{{ raw_database_sap }}`.`notification`
)

SELECT
    notification_key                                     AS externalId
  , name                                                 AS name
  , description                                          AS description
  , status                                               AS status
  , cast(priority_number AS BIGINT)                      AS priority
  , failure_mode_code                                    AS failureModeCode
  , detection_method_code                                AS detectionMethodCode
  , CASE
      WHEN asset_key IS NULL THEN NULL
      ELSE node_reference('{{ instance_space }}', asset_key)
    END                                                  AS asset
  , CASE
      WHEN maintenance_order_key IS NULL THEN NULL
      ELSE node_reference('{{ instance_space }}', maintenance_order_key)
    END                                                  AS maintenanceOrder
  , to_timestamp(start_time)                             AS startTime
  , to_timestamp(end_time)                               AS endTime
  , source_id                                            AS sourceId
  , source_context                                       AS sourceContext
FROM source
WHERE notification_key IS NOT NULL
