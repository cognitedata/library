WITH base AS (
  SELECT
    CAST(batchId AS string) AS external_id,
    CAST(batchId AS string) AS batchId,
    CAST(batchNumber AS string) AS batchNumber,
    CAST(batchState AS string) AS batchState,
    CAST(batchSize AS double) AS batchSize,
    CAST(batchSizeUnit AS string) AS batchSizeUnit,
    CASE
      WHEN startTime IS NULL OR startTime = '' THEN NULL
      ELSE TO_TIMESTAMP(startTime, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    END AS startTime,
    CASE
      WHEN endTime IS NULL OR endTime = '' THEN NULL
      ELSE TO_TIMESTAMP(endTime, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    END AS endTime,
    CAST(recipeExternalId AS string) AS recipeExternalId,
    CAST(siteExternalId AS string) AS siteExternalId,
    CAST(primaryWorkOrderExternalId AS string) AS primaryWorkOrderExternalId
  FROM `{{ rawDatabase }}`.`isa_batch`
  WHERE 1=1 -- full reload: is_new('isa_batch', lastUpdatedTime)
),
withRefs AS (
  SELECT
    external_id,
    batchId,
    batchNumber,
    batchState,
    batchSize,
    batchSizeUnit,
    startTime,
    endTime,
    CASE
      WHEN recipeExternalId IS NULL OR recipeExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', recipeExternalId)
    END AS recipe,
    CASE
      WHEN siteExternalId IS NULL OR siteExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', siteExternalId)
    END AS site,
    CASE
      WHEN primaryWorkOrderExternalId IS NULL OR primaryWorkOrderExternalId = '' THEN NULL
      ELSE ARRAY(node_reference('{{ instance_space }}', primaryWorkOrderExternalId))
    END AS workOrders
  FROM base
)
SELECT
  external_id AS externalId,
  FIRST(batchId, true) AS batchId,
  FIRST(batchNumber, true) AS batchNumber,
  FIRST(batchState, true) AS batchState,
  FIRST(batchSize, true) AS batchSize,
  FIRST(batchSizeUnit, true) AS batchSizeUnit,
  FIRST(startTime, true) AS startTime,
  FIRST(endTime, true) AS endTime,
  FIRST(startTime, true) AS scheduledStartTime,
  FIRST(endTime, true) AS scheduledEndTime,
  FIRST(recipe, true) AS recipe,
  FIRST(site, true) AS site,
  array_distinct(flatten(collectList(workOrders))) AS workOrders
FROM withRefs
GROUP BY external_id
