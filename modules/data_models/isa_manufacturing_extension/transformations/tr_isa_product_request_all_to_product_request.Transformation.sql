WITH base AS (
  SELECT
    CAST(productRequestId AS string) AS external_id,
    CAST(productRequestId AS string) AS productRequestId,
    CAST(requestNumber AS string) AS requestNumber,
    CAST(productRequestName AS string) AS productRequestName,
    CAST(description AS string) AS description,
    CAST(NULLIF(quantityRequested, '') AS double) AS quantityRequested,
    CAST(quantityUnit AS string) AS quantityUnit,
    CAST(NULLIF(priority, '') AS int) AS priority,
    CASE
      WHEN dueDate IS NULL OR dueDate = '' THEN NULL
      ELSE TO_TIMESTAMP(dueDate, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    END AS dueDate,
    CAST(requestStatus AS string) AS requestStatus,
    SPLIT(COALESCE(unitExternalIds, ''), '\\|') AS unitIds,
    SPLIT(COALESCE(fileExternalIds, ''), '\\|') AS fileIds,
    SPLIT(COALESCE(workOrderExternalIds, ''), '\\|') AS workOrderIds
  FROM `{{ rawDatabase }}`.`isa_product_request`
  WHERE is_new('isa_product_request', lastUpdatedTime)
),
withRefs AS (
  SELECT
    external_id,
    productRequestId,
    requestNumber,
    productRequestName,
    description,
    quantityRequested,
    quantityUnit,
    priority,
    dueDate,
    requestStatus,
    FILTER(
      TRANSFORM(
        unitIds,
        x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ instance_space }}', TRIM(x)) END
      ),
      x -> x IS NOT NULL
    ) AS unit,
    FILTER(
      TRANSFORM(
        fileIds,
        x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ instance_space }}', TRIM(x)) END
      ),
      x -> x IS NOT NULL
    ) AS files,
    FILTER(
      TRANSFORM(
        workOrderIds,
        x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ instance_space }}', TRIM(x)) END
      ),
      x -> x IS NOT NULL
    ) AS workOrders
  FROM base
)
SELECT
  external_id AS externalId,
  FIRST(productRequestId, true) AS productRequestId,
  FIRST(requestNumber, true) AS requestNumber,
  FIRST(productRequestName, true) AS name,
  FIRST(description, true) AS description,
  FIRST(quantityRequested, true) AS quantityRequested,
  FIRST(quantityUnit, true) AS quantityUnit,
  FIRST(priority, true) AS priority,
  FIRST(dueDate, true) AS dueDate,
  FIRST(requestStatus, true) AS requestStatus,
  array_distinct(flatten(collectList(unit))) AS unit,
  array_distinct(flatten(collectList(files))) AS files,
  array_distinct(flatten(collectList(workOrders))) AS workOrders
FROM withRefs
GROUP BY external_id
