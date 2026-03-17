SELECT
  CAST(key AS string) AS externalId,
  CAST(quality_result_id AS string) AS quality_result_id,
  CAST(quality_result_name AS string) AS name,
  CAST(description AS string) AS description,
  CAST(test_name AS string) AS test_name,
  CAST(test_method AS string) AS test_method,
  CASE
    WHEN batch_externalId IS NULL OR batch_externalId = '' THEN NULL
    ELSE node_reference('{{ isaInstanceSpace }}', CAST(batch_externalId AS string))
  END AS batch,
  CASE
    WHEN material_lot_externalId IS NULL OR material_lot_externalId = '' THEN NULL
    ELSE node_reference('{{ isaInstanceSpace }}', REPLACE(CAST(material_lot_externalId AS string), '-', '_'))
  END AS material_lot,
  CAST(NULLIF(result_value, '') AS double) AS result_value,
  CAST(result_text AS string) AS result_text,
  CAST(unit_of_measure AS string) AS unit_of_measure,
  CAST(NULLIF(specification_min, '') AS double) AS specification_min,
  CAST(NULLIF(specification_max, '') AS double) AS specification_max,
  TO_TIMESTAMP(test_date, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS test_date,
  CASE
    WHEN analyst_externalId IS NULL OR analyst_externalId = '' THEN NULL
    ELSE node_reference('{{ isaInstanceSpace }}', CAST(analyst_externalId AS string))
  END AS analyst,
  CAST(status AS string) AS status
FROM `ISA_Manufacturing`.`isa_quality_result`
