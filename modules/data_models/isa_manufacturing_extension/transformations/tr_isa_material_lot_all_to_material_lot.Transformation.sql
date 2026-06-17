WITH base AS (
  SELECT
    CAST(key AS string) AS external_id,
    CAST(materialLotName AS string) AS materialLotName,
    CAST(description AS string) AS description,
    CAST(materialLotId AS string) AS materialLotId,
    CAST(lotNumber AS string) AS lotNumber,
    CASE
      WHEN manufactureDate IS NULL OR manufactureDate = '' THEN NULL
      ELSE TO_TIMESTAMP(manufactureDate, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    END AS manufactureDate,
    CASE
      WHEN expiryDate IS NULL OR expiryDate = '' THEN NULL
      ELSE TO_TIMESTAMP(expiryDate, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    END AS expiryDate,
    CAST(materialExternalId AS string) AS materialExternalId
  FROM `{{ rawDatabase }}`.`isa_material_lot`
  WHERE is_new('isa_material_lot', lastUpdatedTime)
),
withRefs AS (
  SELECT
    external_id,
    materialLotName,
    description,
    materialLotId,
    lotNumber,
    manufactureDate,
    expiryDate,
    CASE
      WHEN materialExternalId IS NULL OR materialExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', materialExternalId)
    END AS material
  FROM base
)
SELECT
  external_id AS externalId,
  FIRST(materialLotName, true) AS name,
  FIRST(description, true) AS description,
  FIRST(materialLotId, true) AS materialLotId,
  FIRST(lotNumber, true) AS lotNumber,
  FIRST(manufactureDate, true) AS manufactureDate,
  FIRST(expiryDate, true) AS expiryDate,
  FIRST(material, true) AS material
FROM withRefs
GROUP BY external_id
