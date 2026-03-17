WITH base AS (
  SELECT
    CAST(product_segment_id AS string) AS product_segment_id,
    CAST(product_segment_id AS string) AS external_id,
    CAST(product_segment_name AS string) AS product_segment_name,
    CAST(description AS string) AS segment_description,
    SPLIT(COALESCE(set_points, ''), '\\|') AS set_point_values,
    CAST(NULLIF(time_requirements, '') AS double) AS time_requirements,
    CAST(NULLIF(temperature_requirements, '') AS double) AS temperature_requirements,
    CAST(NULLIF(flow_rate_requirements, '') AS double) AS flow_rate_requirements,
    CAST(NULLIF(pressure_requirements, '') AS double) AS pressure_requirements,
    CAST(NULLIF(ph_requirements, '') AS double) AS ph_requirements,
    CAST(NULLIF(composition_requirements, '') AS double) AS composition_requirements,
    TO_TIMESTAMP(valid_from, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS valid_from_ts,
    TO_TIMESTAMP(valid_to, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS valid_to_ts,
    SPLIT(COALESCE(status, ''), '\\|') AS status_values,
    SPLIT(COALESCE(timeSeries_externalIds, ''), '\\|') AS timeseries_ids,
    SPLIT(COALESCE(material_externalIds, ''), '\\|') AS material_ids,
    SPLIT(COALESCE(equipment_externalIds, ''), '\\|') AS equipment_ids,
    SPLIT(COALESCE(file_externalIds, ''), '\\|') AS file_ids
  FROM `ISA_Manufacturing`.`isa_product_segment`
)
SELECT
  external_id AS externalId,
  product_segment_id,
  product_segment_name AS name,
  segment_description AS description,
  FILTER(
    TRANSFORM(
      set_point_values,
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE TRIM(x) END
    ),
    x -> x IS NOT NULL
  ) AS set_points,
  time_requirements,
  temperature_requirements,
  flow_rate_requirements,
  pressure_requirements,
  ph_requirements,
  composition_requirements,
  valid_from_ts AS valid_from,
  valid_to_ts AS valid_to,
  FILTER(
    TRANSFORM(
      status_values,
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE TRIM(x) END
    ),
    x -> x IS NOT NULL
  ) AS status,
  FILTER(
    TRANSFORM(
      timeseries_ids,
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ isaInstanceSpace }}', TRIM(x)) END
    ),
    x -> x IS NOT NULL
  ) AS timeSeries,
  FILTER(
    TRANSFORM(
      material_ids,
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ isaInstanceSpace }}', REPLACE(TRIM(x), '-', '_')) END
    ),
    x -> x IS NOT NULL
  ) AS material_requirements,
  FILTER(
    TRANSFORM(
      equipment_ids,
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ isaInstanceSpace }}', TRIM(x)) END
    ),
    x -> x IS NOT NULL
  ) AS equipment_requirements,
  FILTER(
    TRANSFORM(
      file_ids,
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ isaInstanceSpace }}', TRIM(x)) END
    ),
    x -> x IS NOT NULL
  ) AS files
FROM base
