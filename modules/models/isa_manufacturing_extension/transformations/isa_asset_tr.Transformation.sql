WITH src AS (
  SELECT
    cast(key as string)                AS externalId,
    cast(name as string)               AS name,
    cast(parent_externalId as string)  AS parent_externalId,
    cast(erp_id as string)             AS erp_id,
  	cast(description as string)        AS description
  FROM `ISA_Manufacturing`.`isa_asset`
)
SELECT
  src.externalId,
  src.name,
  src.description,
  CASE
    WHEN src.parent_externalId IS NULL OR src.parent_externalId = '' THEN NULL
    ELSE node_reference('{{ isaInstanceSpace }}', src.parent_externalId)
  END
  AS parent
FROM src
