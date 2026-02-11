SELECT
  CAST(key AS string) AS externalId,
  CAST(phase_id AS string) AS phase_id,
  CAST(phase_name AS string) AS phase_name,
  CAST(phase_class AS string) AS phase_class,
  CAST(sequence_number AS int) AS sequence_number,
  CAST(phase_state AS string) AS phase_state,
  CASE
    WHEN operation_externalId IS NULL OR operation_externalId = '' THEN NULL
    ELSE node_reference('{{ isaInstanceSpace }}', CAST(operation_externalId AS string))
  END AS operation,
  FILTER(
    TRANSFORM(
      SPLIT(COALESCE(equipment_module_externalIds, ''), '\\|'),
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ isaInstanceSpace }}', TRIM(x)) END
    ),
    x -> x IS NOT NULL
  ) AS equipment_module,
  FILTER(
    TRANSFORM(
      SPLIT(COALESCE(batch_externalIds, ''), '\\|'),
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ isaInstanceSpace }}', TRIM(x)) END
    ),
    x -> x IS NOT NULL
  ) AS batch,
  FILTER(
    TRANSFORM(
      SPLIT(COALESCE(timeseries_externalIds, ''), '\\|'),
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ isaInstanceSpace }}', TRIM(x)) END
    ),
    x -> x IS NOT NULL
  ) AS timeSeries
FROM `ISA_Manufacturing`.`isa_phase`


