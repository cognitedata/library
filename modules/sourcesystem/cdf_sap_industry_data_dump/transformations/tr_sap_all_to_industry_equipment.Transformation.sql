WITH source AS (
  SELECT
      cast(`key` AS STRING)                              AS equipment_key
    , nullif(trim(cast(name AS STRING)), '')             AS name
    , nullif(trim(cast(description AS STRING)), '')      AS description
    , nullif(trim(cast(model AS STRING)), '')            AS model_number
    , nullif(trim(cast(code AS STRING)), '')             AS type_code
    , nullif(trim(cast(asset_externalId AS STRING)), '') AS asset_key
    , nullif(trim(cast(sourceId AS STRING)), '')         AS source_id
    , nullif(trim(cast(source AS STRING)), '')           AS source_context
  FROM `{{ raw_database_sap }}`.`equipment`
)

SELECT
    equipment_key                                        AS externalId
  , name                                                 AS name
  , description                                          AS description
  , model_number                                         AS modelNumber
  , CASE
      WHEN type_code IS NULL THEN NULL
      ELSE node_reference('{{ instance_space }}', type_code)
    END                                                  AS equipmentType
  , CASE
      WHEN asset_key IS NULL THEN NULL
      ELSE node_reference('{{ instance_space }}', asset_key)
    END                                                  AS asset
  , source_id                                            AS sourceId
  , source_context                                       AS sourceContext
FROM source
WHERE equipment_key IS NOT NULL
