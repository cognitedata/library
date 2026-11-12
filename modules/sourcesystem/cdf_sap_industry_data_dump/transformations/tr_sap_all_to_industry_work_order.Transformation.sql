WITH source AS (
  SELECT
      cast(`key` AS STRING)                                          AS work_order_key
    , nullif(trim(cast(name AS STRING)), '')                         AS name
    , nullif(trim(cast(description AS STRING)), '')                  AS description
    , nullif(trim(cast(`order` AS STRING)), '')                      AS work_order_number
    , nullif(trim(cast(systemStatusCode AS STRING)), '')             AS status
    , nullif(trim(cast(maintActivityType AS STRING)), '')            AS type
    , nullif(regexp_extract(trim(cast(SPNPriority AS STRING)), '^([0-9]+)', 1), '') AS priority_number
    , nullif(trim(cast(scheduledStartDateTime AS STRING)), '')       AS scheduled_start
    , nullif(trim(cast(scheduledFinishDateTime AS STRING)), '')      AS scheduled_end
    , nullif(trim(cast(actualOrderStartDateTime AS STRING)), '')     AS actual_start
    , nullif(trim(cast(actualOrderFinishDateTime AS STRING)), '')    AS actual_end
    , nullif(trim(cast(mainAsset_externalId AS STRING)), '')         AS main_asset_key
    , nullif(trim(cast(sourceId AS STRING)), '')                     AS source_id
    , nullif(trim(cast(source AS STRING)), '')                       AS source_context
  FROM `{{ raw_database_sap }}`.`work_order`
)

SELECT
    work_order_key                                       AS externalId
  , name                                                 AS name
  , description                                          AS description
  , work_order_number                                    AS workOrderNumber
  , status                                               AS status
  , type                                                 AS type
  , cast(priority_number AS BIGINT)                      AS priority
  , to_timestamp(scheduled_start)                        AS scheduledStartTime
  , to_timestamp(scheduled_end)                          AS scheduledEndTime
  , to_timestamp(actual_start)                           AS startTime
  , to_timestamp(actual_end)                             AS endTime
  , CASE
      WHEN main_asset_key IS NULL THEN NULL
      ELSE node_reference('{{ instance_space }}', main_asset_key)
    END                                                  AS mainAsset
  , CASE
      WHEN main_asset_key IS NULL THEN NULL
      ELSE array(node_reference('{{ instance_space }}', main_asset_key))
    END                                                  AS assets
  , source_id                                            AS sourceId
  , source_context                                       AS sourceContext
FROM source
WHERE work_order_key IS NOT NULL
