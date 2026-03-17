WITH base AS (
  SELECT
    CAST(product_definition_id AS string) AS product_definition_id,
    CAST(product_definition_id AS string) AS external_id,
    CAST(product_definition_name AS string) AS product_definition_name,
    CAST(description AS string) AS definition_description,
    CAST(version AS string) AS definition_version,
    CAST(status AS string) AS definition_status,
    TO_TIMESTAMP(valid_from, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS valid_from_ts,
    TO_TIMESTAMP(valid_to, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS valid_to_ts,
    SPLIT(COALESCE(unit_externalIds, ''), '\\|') AS unit_ids,
    SPLIT(COALESCE(product_request_externalIds, ''), '\\|') AS request_ids,
    SPLIT(COALESCE(product_segment_externalIds, ''), '\\|') AS segment_ids,
    SPLIT(COALESCE(file_externalIds, ''), '\\|') AS file_ids
  FROM `ISA_Manufacturing`.`isa_product_definition`
)
SELECT
  external_id AS externalId,
  product_definition_id,
  product_definition_name AS name,
  definition_description AS description,
  definition_version AS version,
  definition_status AS status,
  valid_from_ts AS valid_from,
  valid_to_ts AS valid_to,
  FILTER(
    TRANSFORM(
      unit_ids,
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ isaInstanceSpace }}', TRIM(x)) END
    ),
    x -> x IS NOT NULL
  ) AS unit,
  FILTER(
    TRANSFORM(
      request_ids,
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ isaInstanceSpace }}', TRIM(x)) END
    ),
    x -> x IS NOT NULL
  ) AS product_request,
  FILTER(
    TRANSFORM(
      segment_ids,
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ isaInstanceSpace }}', TRIM(x)) END
    ),
    x -> x IS NOT NULL
  ) AS product_segment,
  FILTER(
    TRANSFORM(
      file_ids,
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ isaInstanceSpace }}', TRIM(x)) END
    ),
    x -> x IS NOT NULL
  ) AS files
FROM base
