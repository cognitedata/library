WITH source AS (
  SELECT
      nullif(trim(cast(classId AS STRING)), '')          AS code
    , nullif(trim(cast(className AS STRING)), '')        AS class_name
  FROM `{{ raw_database_sap }}`.`tag`
)

SELECT DISTINCT
    code                                                 AS externalId
  , code                                                 AS code
  , class_name                                           AS name
FROM source
WHERE code IS NOT NULL
  AND class_name IS NOT NULL
