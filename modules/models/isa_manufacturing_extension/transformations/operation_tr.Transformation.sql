SELECT
  CAST(operation_id AS string) AS externalId,
  CAST(operation_id AS string) AS operation_id,
  CAST(operation_name AS string) AS name,
  CAST(description AS string) AS description,
  CAST(startTime AS timestamp) AS startTime,
  CAST(endTime AS timestamp) AS endTime,
  CAST(scheduledStartTime AS timestamp) AS scheduledStartTime,
  CAST(scheduledEndTime AS timestamp) AS scheduledEndTime,
  CAST(sequence_number AS int) AS sequence_number,
  CASE
    WHEN unit_procedure_externalId IS NULL OR unit_procedure_externalId = '' THEN NULL
    ELSE ARRAY(node_reference('{{ isaInstanceSpace }}', CAST(unit_procedure_externalId AS string)))
  END AS unit_procedure,
  CASE
    WHEN batch_externalId IS NULL OR batch_externalId = '' THEN NULL
    ELSE ARRAY(node_reference('{{ isaInstanceSpace }}', CAST(batch_externalId AS string)))
  END AS batch,
  FILTER(
    TRANSFORM(
      SPLIT(COALESCE(asset_externalIds, ''), '\\|'),
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ isaInstanceSpace }}', TRIM(x)) END
    ),
    x -> x IS NOT NULL
  ) AS assets,
  FILTER(
    TRANSFORM(
      SPLIT(COALESCE(equipment_externalIds, ''), '\\|'),
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ isaInstanceSpace }}', TRIM(x)) END
    ),
    x -> x IS NOT NULL
  ) AS equipment,
  FILTER(
    TRANSFORM(
      SPLIT(COALESCE(timeseries_externalIds, ''), '\\|'),
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ isaInstanceSpace }}', TRIM(x)) END
    ),
    x -> x IS NOT NULL
  ) AS timeSeries
FROM `ISA_Manufacturing`.`isa_operation`


