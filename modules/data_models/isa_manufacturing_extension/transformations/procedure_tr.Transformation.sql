SELECT
  CAST(procedure_id AS string) AS externalId,
  CAST(procedure_id AS string) AS procedure_id,
  CAST(procedure_name AS string) AS name,
  CAST(sequence_number AS int) AS sequence_number,
  CASE
    WHEN recipe_externalId IS NULL OR recipe_externalId = '' THEN NULL
    ELSE node_reference('{{ isaInstanceSpace }}', CAST(recipe_externalId AS string))
  END AS recipe,
  CASE
    WHEN batch_externalId IS NULL OR batch_externalId = '' THEN NULL
    ELSE ARRAY(node_reference('{{ isaInstanceSpace }}', CAST(batch_externalId AS string)))
  END AS batch,
  CAST(description AS string) AS description,
  CAST(NULL AS timestamp) AS startTime,
  CAST(NULL AS timestamp) AS endTime,
  CAST(NULL AS timestamp) AS scheduledStartTime,
  CAST(NULL AS timestamp) AS scheduledEndTime
FROM `ISA_Manufacturing`.`isa_procedure`
