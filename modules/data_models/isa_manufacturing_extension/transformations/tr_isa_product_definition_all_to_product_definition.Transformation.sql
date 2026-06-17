WITH base AS (
  SELECT
    CAST(productDefinitionId AS string) AS external_id,
    CAST(productDefinitionId AS string) AS productDefinitionId,
    CAST(productDefinitionName AS string) AS productDefinitionName,
    CAST(description AS string) AS definitionDescription,
    CAST(version AS string) AS definitionVersion,
    CAST(status AS string) AS definitionStatus,
    CASE
      WHEN validFrom IS NULL OR validFrom = '' THEN NULL
      ELSE TO_TIMESTAMP(validFrom, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    END AS validFromTs,
    CASE
      WHEN validTo IS NULL OR validTo = '' THEN NULL
      ELSE TO_TIMESTAMP(validTo, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    END AS validToTs,
    SPLIT(COALESCE(unitExternalIds, ''), '\\|') AS unitIds,
    SPLIT(COALESCE(productRequestExternalIds, ''), '\\|') AS requestIds,
    SPLIT(COALESCE(productSegmentExternalIds, ''), '\\|') AS segmentIds,
    SPLIT(COALESCE(fileExternalIds, ''), '\\|') AS fileIds
  FROM `{{ rawDatabase }}`.`isa_product_definition`
  WHERE is_new('isa_product_definition', lastUpdatedTime)
),
withRefs AS (
  SELECT
    external_id,
    productDefinitionId,
    productDefinitionName,
    definitionDescription,
    definitionVersion,
    definitionStatus,
    validFromTs,
    validToTs,
    FILTER(
      TRANSFORM(
        unitIds,
        x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ instance_space }}', TRIM(x)) END
      ),
      x -> x IS NOT NULL
    ) AS unit,
    FILTER(
      TRANSFORM(
        requestIds,
        x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ instance_space }}', TRIM(x)) END
      ),
      x -> x IS NOT NULL
    ) AS productRequest,
    FILTER(
      TRANSFORM(
        segmentIds,
        x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ instance_space }}', TRIM(x)) END
      ),
      x -> x IS NOT NULL
    ) AS productSegment,
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
  FIRST(productDefinitionId, true) AS productDefinitionId,
  FIRST(productDefinitionName, true) AS name,
  FIRST(definitionDescription, true) AS description,
  FIRST(definitionVersion, true) AS version,
  FIRST(definitionStatus, true) AS status,
  FIRST(validFromTs, true) AS validFrom,
  FIRST(validToTs, true) AS validTo,
  array_distinct(flatten(collectList(unit))) AS unit,
  array_distinct(flatten(collectList(productRequest))) AS productRequest,
  array_distinct(flatten(collectList(productSegment))) AS productSegment,
  array_distinct(flatten(collectList(files))) AS files
FROM withRefs
GROUP BY external_id
