WITH base AS (
  SELECT
    CAST(procedureId AS string) AS external_id,
    CAST(procedureId AS string) AS procedureId,
    CAST(procedureName AS string) AS procedureName,
    CAST(sequenceNumber AS int) AS sequenceNumber,
    CAST(description AS string) AS procedureDescription,
    CAST(recipeExternalId AS string) AS recipeExternalId,
    CAST(batchExternalId AS string) AS batchExternalId
  FROM `{{ rawDatabase }}`.`isa_procedure`
  WHERE 1=1 -- full reload: is_new('isa_procedure', lastUpdatedTime)
),
withRefs AS (
  SELECT
    external_id,
    procedureId,
    procedureName,
    sequenceNumber,
    procedureDescription,
    CASE
      WHEN recipeExternalId IS NULL OR recipeExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', recipeExternalId)
    END AS recipe,
    CASE
      WHEN batchExternalId IS NULL OR batchExternalId = '' THEN NULL
      ELSE ARRAY(node_reference('{{ instance_space }}', batchExternalId))
    END AS batch
  FROM base
)
SELECT
  external_id AS externalId,
  FIRST(procedureId, true) AS procedureId,
  FIRST(procedureName, true) AS name,
  FIRST(sequenceNumber, true) AS sequenceNumber,
  FIRST(procedureDescription, true) AS description,
  CAST(NULL AS timestamp) AS startTime,
  CAST(NULL AS timestamp) AS endTime,
  CAST(NULL AS timestamp) AS scheduledStartTime,
  CAST(NULL AS timestamp) AS scheduledEndTime,
  FIRST(recipe, true) AS recipe,
  array_distinct(flatten(collectList(batch))) AS batch
FROM withRefs
GROUP BY external_id
