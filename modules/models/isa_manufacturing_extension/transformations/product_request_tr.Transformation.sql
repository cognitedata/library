WITH base AS (
  SELECT
    CAST(product_request_id AS string) AS product_request_id,
    CAST(product_request_id AS string) AS external_id,
    CAST(request_number AS string) AS request_number,
    CAST(product_request_name AS string) AS product_request_name,
    CAST(description AS string) AS description,
    -- CAST(source_system AS string) AS source_system,
    CAST(NULLIF(quantity_requested, '') AS double) AS quantity_requested,
    CAST(quantity_unit AS string) AS quantity_unit,
    CAST(NULLIF(priority, '') AS int) AS priority,
    TO_TIMESTAMP(due_date, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS due_date,
    CAST(request_status AS string) AS request_status,
    SPLIT(COALESCE(unit_externalIds, ''), '\\|') AS unit_ids,
    SPLIT(COALESCE(file_externalIds, ''), '\\|') AS file_ids,
    SPLIT(COALESCE(work_order_externalIds, ''), '\\|') AS work_order_ids
  FROM `ISA_Manufacturing`.`isa_product_request`
)
SELECT
  external_id AS externalId,
  product_request_id,
  request_number,
  product_request_name AS name,
  description,
  -- source_system AS source,
  quantity_requested,
  quantity_unit,
  priority,
  due_date,
  request_status,
  FILTER(
    TRANSFORM(
      unit_ids,
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ isaInstanceSpace }}', TRIM(x)) END
    ),
    x -> x IS NOT NULL
  ) AS unit,
  FILTER(
    TRANSFORM(
      file_ids,
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ isaInstanceSpace }}', TRIM(x)) END
    ),
    x -> x IS NOT NULL
  ) AS files,
  FILTER(
    TRANSFORM(
      work_order_ids,
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ isaInstanceSpace }}', TRIM(x)) END
    ),
    x -> x IS NOT NULL
  ) AS work_orders
FROM base
