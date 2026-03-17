SELECT
  CAST(unit_procedure_id AS string) AS externalId,
  CAST(unit_procedure_id AS string) AS unit_procedure_id,
  CAST(unit_procedure_name AS string) AS unit_procedure_name,
  -- CAST(unit_procedure_name AS string) AS name,
  CAST(sequence_number AS int) AS sequence_number,
  CASE
    WHEN procedure_externalId IS NULL OR procedure_externalId = '' THEN NULL
    ELSE node_reference('{{ isaInstanceSpace }}', CAST(procedure_externalId AS string))
  END AS procedure,
  CASE
    WHEN unit_externalId IS NULL OR unit_externalId = '' THEN NULL
    ELSE node_reference('{{ isaInstanceSpace }}', CAST(unit_externalId AS string))
  END AS unit
FROM `ISA_Manufacturing`.`isa_unit_procedure`
