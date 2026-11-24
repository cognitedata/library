WITH source AS (
  SELECT
      nullif(trim(cast(code AS STRING)), '')              AS code
    , nullif(trim(cast(`class` AS STRING)), '')           AS class_name
    , nullif(trim(cast(equipmentClass AS STRING)), '')    AS equipment_class
    , nullif(trim(cast(standard AS STRING)), '')          AS standard
    , nullif(trim(cast(standardReference AS STRING)), '') AS standard_reference
  FROM `{{ raw_database_sap }}`.`equipment`
)

SELECT DISTINCT
    code                                                  AS externalId
  , code                                                  AS code
  , class_name                                            AS name
  , equipment_class                                       AS equipmentClass
  , standard                                              AS standard
  , standard_reference                                    AS standardReference
FROM source
WHERE code IS NOT NULL
