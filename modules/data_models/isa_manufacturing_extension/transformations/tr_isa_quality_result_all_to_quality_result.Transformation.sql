WITH base AS (
  SELECT
    CAST(key AS string) AS external_id,
    CAST(qualityResultId AS string) AS qualityResultId,
    CAST(qualityResultName AS string) AS qualityResultName,
    CAST(description AS string) AS description,
    CAST(testName AS string) AS testName,
    CAST(testMethod AS string) AS testMethod,
    CAST(batchExternalId AS string) AS batchExternalId,
    CAST(materialLotExternalId AS string) AS materialLotExternalId,
    CAST(NULLIF(resultValue, '') AS double) AS resultValue,
    CAST(resultText AS string) AS resultText,
    CAST(unitOfMeasure AS string) AS unitOfMeasure,
    CAST(NULLIF(specificationMin, '') AS double) AS specificationMin,
    CAST(NULLIF(specificationMax, '') AS double) AS specificationMax,
    CASE
      WHEN testDate IS NULL OR testDate = '' THEN NULL
      ELSE TO_TIMESTAMP(testDate, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    END AS testDate,
    CAST(analystExternalId AS string) AS analystExternalId,
    CAST(status AS string) AS status
  FROM `{{ rawDatabase }}`.`isa_quality_result`
  WHERE is_new('isa_quality_result', lastUpdatedTime)
),
withRefs AS (
  SELECT
    external_id,
    qualityResultId,
    qualityResultName,
    description,
    testName,
    testMethod,
    resultValue,
    resultText,
    unitOfMeasure,
    specificationMin,
    specificationMax,
    testDate,
    status,
    CASE
      WHEN batchExternalId IS NULL OR batchExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', batchExternalId)
    END AS batch,
    CASE
      WHEN materialLotExternalId IS NULL OR materialLotExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', REPLACE(materialLotExternalId, '-', '_'))
    END AS materialLot,
    CASE
      WHEN analystExternalId IS NULL OR analystExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', analystExternalId)
    END AS analyst
  FROM base
)
SELECT
  external_id AS externalId,
  FIRST(qualityResultId, true) AS qualityResultId,
  FIRST(qualityResultName, true) AS name,
  FIRST(description, true) AS description,
  FIRST(testName, true) AS testName,
  FIRST(testMethod, true) AS testMethod,
  FIRST(batch, true) AS batch,
  FIRST(materialLot, true) AS materialLot,
  FIRST(resultValue, true) AS resultValue,
  FIRST(resultText, true) AS resultText,
  FIRST(unitOfMeasure, true) AS unitOfMeasure,
  FIRST(specificationMin, true) AS specificationMin,
  FIRST(specificationMax, true) AS specificationMax,
  FIRST(testDate, true) AS testDate,
  FIRST(analyst, true) AS analyst,
  FIRST(status, true) AS status
FROM withRefs
GROUP BY external_id
