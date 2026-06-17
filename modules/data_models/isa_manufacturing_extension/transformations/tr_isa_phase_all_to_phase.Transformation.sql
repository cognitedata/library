WITH base AS (
  SELECT
    CAST(key AS string) AS external_id,
    CAST(phaseId AS string) AS phaseId,
    CAST(phaseName AS string) AS phaseName,
    CAST(phaseClass AS string) AS phaseClass,
    CAST(sequenceNumber AS int) AS sequenceNumber,
    CAST(phaseState AS string) AS phaseState,
    CAST(operationExternalId AS string) AS operationExternalId,
    SPLIT(COALESCE(equipmentModuleExternalIds, ''), '\\|') AS equipmentModuleIds,
    SPLIT(COALESCE(batchExternalIds, ''), '\\|') AS batchIds,
    SPLIT(COALESCE(timeSeriesExternalIds, ''), '\\|') AS timeseriesIds
  FROM `{{ rawDatabase }}`.`isa_phase`
  WHERE 1=1 -- full reload: is_new('isa_phase', lastUpdatedTime)
),
withRefs AS (
  SELECT
    external_id,
    phaseId,
    phaseName,
    phaseClass,
    sequenceNumber,
    phaseState,
    CASE
      WHEN operationExternalId IS NULL OR operationExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', operationExternalId)
    END AS operation,
    FILTER(
      TRANSFORM(
        equipmentModuleIds,
        x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ instance_space }}', TRIM(x)) END
      ),
      x -> x IS NOT NULL
    ) AS equipmentModule,
    FILTER(
      TRANSFORM(
        batchIds,
        x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ instance_space }}', TRIM(x)) END
      ),
      x -> x IS NOT NULL
    ) AS batch,
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
  FIRST(phaseId, true) AS phaseId,
  FIRST(phaseName, true) AS phaseName,
  FIRST(phaseClass, true) AS phaseClass,
  FIRST(sequenceNumber, true) AS sequenceNumber,
  FIRST(phaseState, true) AS phaseState,
  FIRST(operation, true) AS operation,
  array_distinct(flatten(collect_list(equipmentModule))) AS equipmentModule,
  array_distinct(flatten(collect_list(batch))) AS batch,
  array_distinct(flatten(collect_list(timeSeries))) AS timeSeries
FROM withRefs
GROUP BY external_id
