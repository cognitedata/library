WITH base AS (
  SELECT
    CAST(productSegmentId AS string) AS external_id,
    CAST(productSegmentId AS string) AS productSegmentId,
    CAST(productSegmentName AS string) AS productSegmentName,
    CAST(description AS string) AS segmentDescription,
    SPLIT(COALESCE(setPoints, ''), '\\|') AS setPointValues,
    CAST(NULLIF(timeRequirements, '') AS double) AS timeRequirements,
    CAST(NULLIF(temperatureRequirements, '') AS double) AS temperatureRequirements,
    CAST(NULLIF(flowRateRequirements, '') AS double) AS flowRateRequirements,
    CAST(NULLIF(pressureRequirements, '') AS double) AS pressureRequirements,
    CAST(NULLIF(phRequirements, '') AS double) AS phRequirements,
    CAST(NULLIF(compositionRequirements, '') AS double) AS compositionRequirements,
    CASE
      WHEN validFrom IS NULL OR validFrom = '' THEN NULL
      ELSE TO_TIMESTAMP(validFrom, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    END AS validFromTs,
    CASE
      WHEN validTo IS NULL OR validTo = '' THEN NULL
      ELSE TO_TIMESTAMP(validTo, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    END AS validToTs,
    SPLIT(COALESCE(status, ''), '\\|') AS statusValues,
    SPLIT(COALESCE(timeSeriesExternalIds, ''), '\\|') AS timeseriesIds,
    SPLIT(COALESCE(materialExternalIds, ''), '\\|') AS materialIds,
    SPLIT(COALESCE(equipmentExternalIds, ''), '\\|') AS equipmentIds,
    SPLIT(COALESCE(fileExternalIds, ''), '\\|') AS fileIds
  FROM `{{ rawDatabase }}`.`isa_product_segment`
  WHERE is_new('isa_product_segment', lastUpdatedTime)
),
withRefs AS (
  SELECT
    external_id,
    productSegmentId,
    productSegmentName,
    segmentDescription,
    timeRequirements,
    temperatureRequirements,
    flowRateRequirements,
    pressureRequirements,
    phRequirements,
    compositionRequirements,
    validFromTs,
    validToTs,
    FILTER(
      TRANSFORM(
        setPointValues,
        x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE TRIM(x) END
      ),
      x -> x IS NOT NULL
    ) AS setPoints,
    FILTER(
      TRANSFORM(
        statusValues,
        x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE TRIM(x) END
      ),
      x -> x IS NOT NULL
    ) AS status,
    FILTER(
      TRANSFORM(
        timeseriesIds,
        x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ instance_space }}', TRIM(x)) END
      ),
      x -> x IS NOT NULL
    ) AS timeSeries,
    FILTER(
      TRANSFORM(
        materialIds,
        x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ instance_space }}', REPLACE(TRIM(x), '-', '_')) END
      ),
      x -> x IS NOT NULL
    ) AS materialRequirements,
    FILTER(
      TRANSFORM(
        equipmentIds,
        x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ instance_space }}', TRIM(x)) END
      ),
      x -> x IS NOT NULL
    ) AS equipmentRequirements,
    FILTER(
      TRANSFORM(
        fileIds,
        x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ instance_space }}', TRIM(x)) END
      ),
      x -> x IS NOT NULL
    ) AS files
  FROM base
)
SELECT
  external_id AS externalId,
  FIRST(productSegmentId, true) AS productSegmentId,
  FIRST(productSegmentName, true) AS name,
  FIRST(segmentDescription, true) AS description,
  array_distinct(flatten(collectList(setPoints))) AS setPoints,
  FIRST(timeRequirements, true) AS timeRequirements,
  FIRST(temperatureRequirements, true) AS temperatureRequirements,
  FIRST(flowRateRequirements, true) AS flowRateRequirements,
  FIRST(pressureRequirements, true) AS pressureRequirements,
  FIRST(phRequirements, true) AS phRequirements,
  FIRST(compositionRequirements, true) AS compositionRequirements,
  FIRST(validFromTs, true) AS validFrom,
  FIRST(validToTs, true) AS validTo,
  array_distinct(flatten(collectList(status))) AS status,
  array_distinct(flatten(collectList(timeSeries))) AS timeSeries,
  array_distinct(flatten(collectList(materialRequirements))) AS materialRequirements,
  array_distinct(flatten(collectList(equipmentRequirements))) AS equipmentRequirements,
  array_distinct(flatten(collectList(files))) AS files
FROM withRefs
GROUP BY external_id
