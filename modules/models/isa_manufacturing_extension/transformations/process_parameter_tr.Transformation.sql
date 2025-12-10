WITH base AS (
  SELECT
    CAST(process_parameter_id AS string) AS process_parameter_id,
    CAST(process_parameter_id AS string) AS external_id,
    CAST(process_parameter_name AS string) AS process_parameter_name,
    CAST(description AS string) AS parameter_description,
    CAST(unit_of_measure AS string) AS unit_of_measure,
    CAST(NULLIF(target_value, '') AS double) AS target_value,
    CAST(NULLIF(min_value, '') AS double) AS min_value,
    CAST(NULLIF(max_value, '') AS double) AS max_value,
    SPLIT(COALESCE(phase_externalIds, ''), '\\|') AS phase_ids,
    SPLIT(COALESCE(product_segment_externalIds, ''), '\\|') AS segment_ids
  FROM `ISA_Manufacturing`.`isa_process_parameter`
)
SELECT
  external_id AS externalId,
  process_parameter_id,
  process_parameter_name AS name,
  parameter_description AS description,
  unit_of_measure,
  target_value,
  min_value,
  max_value,
  FILTER(
    TRANSFORM(
      phase_ids,
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ isaInstanceSpace }}', REPLACE(TRIM(x), '-', '_')) END
    ),
    x -> x IS NOT NULL
  ) AS phases,
  FILTER(
    TRANSFORM(
      segment_ids,
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ isaInstanceSpace }}', TRIM(x)) END
    ),
    x -> x IS NOT NULL
  ) AS product_segment
FROM base
