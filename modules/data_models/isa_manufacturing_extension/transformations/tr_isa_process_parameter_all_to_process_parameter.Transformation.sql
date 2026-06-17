WITH base AS (
  SELECT
    CAST(processParameterId AS string) AS external_id,
    CAST(processParameterId AS string) AS processParameterId,
    CAST(processParameterName AS string) AS processParameterName,
    CAST(description AS string) AS parameterDescription,
    CAST(unitOfMeasure AS string) AS unitOfMeasure,
    CAST(NULLIF(targetValue, '') AS double) AS targetValue,
    CAST(NULLIF(minValue, '') AS double) AS minValue,
    CAST(NULLIF(maxValue, '') AS double) AS maxValue,
    SPLIT(COALESCE(phaseExternalIds, ''), '\\|') AS phaseIds,
    SPLIT(COALESCE(productSegmentExternalIds, ''), '\\|') AS segmentIds
  FROM `{{ rawDatabase }}`.`isa_process_parameter`
  WHERE is_new('isa_process_parameter', lastUpdatedTime)
),
withRefs AS (
  SELECT
    external_id,
    processParameterId,
    processParameterName,
    parameterDescription,
    unitOfMeasure,
    targetValue,
    minValue,
    maxValue,
    FILTER(
      TRANSFORM(
        phaseIds,
        x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ instance_space }}', REPLACE(TRIM(x), '-', '_')) END
      ),
      x -> x IS NOT NULL
    ) AS phases,
    FILTER(
      TRANSFORM(
        segmentIds,
        x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ instance_space }}', TRIM(x)) END
      ),
      x -> x IS NOT NULL
    ) AS productSegment
  FROM base
)
SELECT
  external_id AS externalId,
  FIRST(processParameterId, true) AS processParameterId,
  FIRST(processParameterName, true) AS name,
  FIRST(parameterDescription, true) AS description,
  FIRST(unitOfMeasure, true) AS unitOfMeasure,
  FIRST(targetValue, true) AS targetValue,
  FIRST(minValue, true) AS minValue,
  FIRST(maxValue, true) AS maxValue,
  array_distinct(flatten(collectList(phases))) AS phases,
  array_distinct(flatten(collectList(productSegment))) AS productSegment
FROM withRefs
GROUP BY external_id
