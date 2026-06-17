WITH base AS (
  SELECT
    cast(key as string) AS external_id,
    cast(name as string) AS name,
    cast(description as string) AS description,
    cast(`unit` as string) AS sourceUnit,
    cast(isString as string) AS isString,
    cast(isStep as string) AS isStep,
    cast(assetExternalId as string) AS assetExternalId,
    cast(equipmentExternalId as string) AS equipmentExternalId
  FROM `{{ rawDatabase }}`.`isa_timeseries`
  WHERE 1=1 -- full reload: is_new('isa_timeseries', lastUpdatedTime)
),
withRefs AS (
  SELECT
    external_id,
    name,
    description,
    sourceUnit AS sourceUnit,
    CASE
      WHEN lower(isString) = 'true' THEN 'string'
      ELSE 'numeric'
    END AS type,
    CASE
      WHEN lower(isStep) = 'true' THEN true
      ELSE false
    END AS isStep,
    CASE
      WHEN lower(isString) = 'true' THEN NULL
      WHEN sourceUnit IS NULL OR trim(sourceUnit) = '' THEN NULL
      WHEN try_get_unit(sourceUnit) IS NOT NULL THEN node_reference('cdf_cdm_units', try_get_unit(sourceUnit))
      ELSE NULL
    END AS unit,
    CASE
      WHEN assetExternalId IS NULL OR trim(assetExternalId) = '' THEN NULL
      ELSE array(node_reference('{{ instance_space }}', trim(assetExternalId)))
    END AS assets,
    CASE
      WHEN equipmentExternalId IS NULL OR trim(equipmentExternalId) = '' THEN NULL
      ELSE array(node_reference('{{ instance_space }}', trim(equipmentExternalId)))
    END AS equipment
  FROM base
)
SELECT
  external_id AS externalId,
  FIRST(name, true) AS name,
  FIRST(description, true) AS description,
  FIRST(unit, true) AS unit,
  FIRST(sourceUnit, true) AS sourceUnit,
  FIRST(type, true) AS type,
  FIRST(isStep, true) AS isStep,
  array_distinct(flatten(collect_list(assets))) AS assets,
  array_distinct(flatten(collect_list(equipment))) AS equipment
FROM withRefs
GROUP BY external_id
