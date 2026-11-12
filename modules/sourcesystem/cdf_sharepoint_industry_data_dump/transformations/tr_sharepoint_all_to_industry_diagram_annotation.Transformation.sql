WITH source AS (
  SELECT
      cast(`key` AS STRING)                                            AS annotation_key
    , cast(startNodeExternalId AS STRING)                              AS start_key
    , cast(endNodeExternalId AS STRING)                                AS end_key
    , cast(`type` AS STRING)                                           AS annotation_type
    , nullif(trim(cast(status AS STRING)), '')                         AS status
    , cast(nullif(trim(cast(confidence AS STRING)), '') AS DOUBLE)     AS confidence
    , nullif(trim(cast(name AS STRING)), '')                           AS name
    , nullif(trim(cast(startNodeText AS STRING)), '')                  AS start_node_text
    , cast(nullif(trim(cast(startNodeXMax AS STRING)), '') AS DOUBLE)  AS start_node_x_max
    , cast(nullif(trim(cast(startNodeXMin AS STRING)), '') AS DOUBLE)  AS start_node_x_min
    , cast(nullif(trim(cast(startNodeYMax AS STRING)), '') AS DOUBLE)  AS start_node_y_max
    , cast(nullif(trim(cast(startNodeYMin AS STRING)), '') AS DOUBLE)  AS start_node_y_min
    , cast(nullif(trim(cast(startNodePageNumber AS STRING)), '') AS INT) AS start_node_page_number
    , nullif(trim(cast(sourceCreatedTime AS STRING)), '')              AS source_created_time
    , nullif(trim(cast(sourceCreatedUser AS STRING)), '')              AS source_created_user
    , nullif(trim(cast(sourceUpdatedTime AS STRING)), '')              AS source_updated_time
    , nullif(trim(cast(sourceUpdatedUser AS STRING)), '')              AS source_updated_user
  FROM `{{ raw_database_sharepoint }}`.`diagram_annotation`
  WHERE lower(cast(instanceType AS STRING)) = 'edge'
)

SELECT
    annotation_key                                       AS externalId
  , 'edge'                                               AS instanceType
  , '{{ instance_space }}'                               AS space
  , node_reference('{{ instance_space }}', start_key)    AS startNode
  , node_reference('{{ instance_space }}', end_key)      AS endNode
  , type_reference('cdf_cdm', annotation_type)           AS `type`
  , status                                               AS status
  , confidence                                           AS confidence
  , name                                                 AS name
  , start_node_text                                      AS startNodeText
  , start_node_x_max                                     AS startNodeXMax
  , start_node_x_min                                     AS startNodeXMin
  , start_node_y_max                                     AS startNodeYMax
  , start_node_y_min                                     AS startNodeYMin
  , start_node_page_number                               AS startNodePageNumber
  , to_timestamp(source_created_time)                    AS sourceCreatedTime
  , source_created_user                                  AS sourceCreatedUser
  , to_timestamp(source_updated_time)                    AS sourceUpdatedTime
  , source_updated_user                                  AS sourceUpdatedUser
FROM source
WHERE annotation_key IS NOT NULL
  AND start_key IS NOT NULL
  AND end_key IS NOT NULL
