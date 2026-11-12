WITH source AS (
  SELECT
      nullif(trim(cast(documentTypeCode AS STRING)), '') AS code
  FROM `{{ raw_database_sharepoint }}`.`file`
)

SELECT DISTINCT
    code AS externalId
  , code AS code
  , code AS name
FROM source
WHERE code IS NOT NULL
