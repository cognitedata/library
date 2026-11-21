WITH source AS (
  SELECT
      cast(`key` AS STRING)                                                 AS operation_key
    , nullif(trim(cast(name AS STRING)), '')                                AS name
    , nullif(trim(cast(description AS STRING)), '')                         AS description
    , coalesce(
        nullif(trim(cast(userStatusCode AS STRING)), '')
      , nullif(trim(cast(systemStatusCode AS STRING)), '')
      )                                                                     AS status
    , nullif(trim(cast(operationCounter AS STRING)), '')                    AS sequence_number
    , nullif(trim(cast(wctrDiscipline AS STRING)), '')                      AS main_discipline
    , nullif(trim(cast(plannedWork AS STRING)), '')                         AS planned_work
    , nullif(trim(cast(maintenanceOrder_externalId AS STRING)), '')         AS maintenance_order_key
    , nullif(trim(cast(mainAsset_externalId AS STRING)), '')                AS main_asset_key
    , nullif(trim(cast(startTime AS STRING)), '')                           AS start_time
    , nullif(trim(cast(endTime AS STRING)), '')                             AS end_time
    , nullif(trim(cast(sourceId AS STRING)), '')                            AS source_id
    , nullif(trim(cast(source AS STRING)), '')                              AS source_context
  FROM `{{ raw_database_sap }}`.`work_order_operation`
)

SELECT
    operation_key                                        AS externalId
  , name                                                 AS name
  , description                                          AS description
  , status                                               AS status
  , cast(sequence_number AS BIGINT)                      AS sequence
  , main_discipline                                      AS mainDiscipline
  , cast(planned_work AS DOUBLE)                         AS personHours
  , CASE
      WHEN maintenance_order_key IS NULL THEN NULL
      ELSE node_reference('{{ instance_space }}', maintenance_order_key)
    END                                                  AS maintenanceOrder
  , CASE
      WHEN main_asset_key IS NULL THEN NULL
      ELSE node_reference('{{ instance_space }}', main_asset_key)
    END                                                  AS mainAsset
  , CASE
      WHEN main_asset_key IS NULL THEN NULL
      ELSE array(node_reference('{{ instance_space }}', main_asset_key))
    END                                                  AS assets
  , to_timestamp(start_time)                             AS startTime
  , to_timestamp(end_time)                               AS endTime
  , source_id                                            AS sourceId
  , source_context                                       AS sourceContext
FROM source
WHERE operation_key IS NOT NULL
