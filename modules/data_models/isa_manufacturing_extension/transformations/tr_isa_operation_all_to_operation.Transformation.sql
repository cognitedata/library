WITH base AS (
  SELECT
    CAST(operationId AS string) AS external_id,
    CAST(operationId AS string) AS operationId,
    CAST(operationName AS string) AS operationName,
    CAST(description AS string) AS operationDescription,
    CAST(startTime AS timestamp) AS startTime,
    CAST(endTime AS timestamp) AS endTime,
    CAST(scheduledStartTime AS timestamp) AS scheduledStartTime,
    CAST(scheduledEndTime AS timestamp) AS scheduledEndTime,
    CAST(sequenceNumber AS int) AS sequenceNumber,
    CAST(unitProcedureExternalId AS string) AS unitProcedureExternalId,
    CAST(batchExternalId AS string) AS batchExternalId,
    SPLIT(COALESCE(assetExternalIds, ''), '\\|') AS assetIds,
    SPLIT(COALESCE(equipmentExternalIds, ''), '\\|') AS equipmentIds,
    SPLIT(COALESCE(timeSeriesExternalIds, ''), '\\|') AS timeseriesIds
  FROM `{{ rawDatabase }}`.`isa_operation`
  WHERE is_new('{{ rawDatabase }}', 'isa_operation')
),
withRefs AS (
  SELECT
    external_id,
    operationId,
    operationName,
    operationDescription,
    startTime,
    endTime,
    scheduledStartTime,
    scheduledEndTime,
    sequenceNumber,
    CASE
      WHEN unitProcedureExternalId IS NULL OR unitProcedureExternalId = '' THEN NULL
      ELSE ARRAY(node_reference('{{ instance_space }}', unitProcedureExternalId))
    END AS unitProcedure,
    CASE
      WHEN batchExternalId IS NULL OR batchExternalId = '' THEN NULL
      ELSE ARRAY(node_reference('{{ instance_space }}', batchExternalId))
    END AS batch,
    FILTER(
      TRANSFORM(
        assetIds,
        x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ instance_space }}', TRIM(x)) END
      ),
      x -> x IS NOT NULL
    ) AS assets,
    FILTER(
      TRANSFORM(
        equipmentIds,
        x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ instance_space }}', TRIM(x)) END
      ),
      x -> x IS NOT NULL
    ) AS equipment,
    FILTER(
      TRANSFORM(
        timeseriesIds,
        x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ instance_space }}', TRIM(x)) END
      ),
      x -> x IS NOT NULL
    ) AS timeSeries
  FROM base
)
SELECT
  external_id AS externalId,
  FIRST(operationId, true) AS operationId,
  FIRST(operationName, true) AS name,
  FIRST(operationDescription, true) AS description,
  FIRST(startTime, true) AS startTime,
  FIRST(endTime, true) AS endTime,
  FIRST(scheduledStartTime, true) AS scheduledStartTime,
  FIRST(scheduledEndTime, true) AS scheduledEndTime,
  FIRST(sequenceNumber, true) AS sequenceNumber,
  array_distinct(flatten(collectList(unitProcedure))) AS unitProcedure,
  array_distinct(flatten(collectList(batch))) AS batch,
  array_distinct(flatten(collectList(assets))) AS assets,
  array_distinct(flatten(collectList(equipment))) AS equipment,
  array_distinct(flatten(collectList(timeSeries))) AS timeSeries
FROM withRefs
GROUP BY external_id
