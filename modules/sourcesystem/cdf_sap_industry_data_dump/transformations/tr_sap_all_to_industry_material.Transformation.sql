WITH source AS (
  SELECT
      cast(`key` AS STRING)                                    AS material_key
    , nullif(trim(cast(name AS STRING)), '')                   AS name
    , nullif(trim(cast(materialType AS STRING)), '')           AS material_type
    , nullif(trim(cast(equipment_externalId AS STRING)), '')   AS equipment_key
    , nullif(trim(cast(asset_externalId AS STRING)), '')       AS asset_key
  FROM `{{ raw_database_sap }}`.`material`
)

SELECT
    material_key                                         AS externalId
  , name                                                 AS name
  , material_type                                        AS materialType
  , CASE
      WHEN equipment_key IS NULL THEN NULL
      ELSE node_reference('{{ instance_space }}', equipment_key)
    END                                                  AS equipment
  , CASE
      WHEN asset_key IS NULL THEN NULL
      ELSE node_reference('{{ instance_space }}', asset_key)
    END                                                  AS asset
FROM source
WHERE material_key IS NOT NULL
