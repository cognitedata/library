WITH src AS (
  SELECT
    cast(key as string)                AS externalId,
    cast(name as string)               AS name,
    cast(description as string)        AS description,
    if(try_get_unit(`unit`) IS NOT NULL, node_reference('cdf_cdm_units', try_get_unit(`unit`)), NULL) as unit,
    `unit` as sourceUnit,
  	'numeric' as type,
    array(node_reference('{{ isaInstanceSpace }}', `asset_externalId`)) as assets,
    array(node_reference('{{ isaInstanceSpace }}', `equipment_externalId`)) as equipment,
    CASE lower(isStep)
      WHEN 'true' THEN true
      ELSE false
    END AS isStep
  FROM `ISA_Manufacturing`.`isa_timeseries`
)
SELECT
  externalId,
  name,
  description,
  unit,
  sourceUnit,
  type,
  isStep,
  assets,
  equipment
FROM src
