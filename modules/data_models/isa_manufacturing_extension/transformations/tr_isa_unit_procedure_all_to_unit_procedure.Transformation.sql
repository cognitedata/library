WITH base AS (
  SELECT
    CAST(unitProcedureId AS string) AS external_id,
    CAST(unitProcedureId AS string) AS unitProcedureId,
    CAST(unitProcedureName AS string) AS unitProcedureName,
    CAST(sequenceNumber AS int) AS sequenceNumber,
    CAST(procedureExternalId AS string) AS procedureExternalId,
    CAST(unitExternalId AS string) AS unitExternalId
  FROM `{{ rawDatabase }}`.`isa_unit_procedure`
  WHERE is_new('isa_unit_procedure', lastUpdatedTime)
)
SELECT
  external_id AS externalId,
  FIRST(unitProcedureId, true) AS unitProcedureId,
  FIRST(unitProcedureName, true) AS unitProcedureName,
  FIRST(sequenceNumber, true) AS sequenceNumber,
  FIRST(
    CASE
      WHEN procedureExternalId IS NULL OR procedureExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', procedureExternalId)
    END,
    true
  ) AS procedure,
  FIRST(
    CASE
      WHEN unitExternalId IS NULL OR unitExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', unitExternalId)
    END,
    true
  ) AS unit
FROM base
GROUP BY external_id
