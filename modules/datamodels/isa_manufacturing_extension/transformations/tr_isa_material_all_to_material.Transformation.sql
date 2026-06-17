WITH base AS (
  SELECT
    CAST(materialId AS string) AS external_id,
    CAST(materialId AS string) AS materialId,
    CAST(materialName AS string) AS materialName,
    CAST(materialClass AS string) AS materialClass,
    CAST(description AS string) AS materialDescription,
    CAST(primaryRecipeExternalId AS string) AS primaryRecipeExternalId,
    CAST(batchExternalId AS string) AS batchExternalId
  FROM `{{ rawDatabase }}`.`isa_material`
  WHERE 1=1 -- full reload: is_new('isa_material', lastUpdatedTime)
),
withRefs AS (
  SELECT
    external_id,
    materialId,
    materialName,
    materialClass,
    materialDescription,
    CASE
      WHEN primaryRecipeExternalId IS NULL OR primaryRecipeExternalId = '' THEN NULL
      ELSE ARRAY(node_reference('{{ instance_space }}', primaryRecipeExternalId))
    END AS recipe,
    CASE
      WHEN batchExternalId IS NULL OR batchExternalId = '' THEN NULL
      ELSE ARRAY(node_reference('{{ instance_space }}', batchExternalId))
    END AS batch
  FROM base
)
SELECT
  external_id AS externalId,
  FIRST(materialId, true) AS materialId,
  FIRST(materialName, true) AS name,
  FIRST(materialClass, true) AS materialClass,
  FIRST(materialDescription, true) AS description,
  array_distinct(flatten(collect_list(recipe))) AS recipe,
  array_distinct(flatten(collect_list(batch))) AS batch
FROM withRefs
GROUP BY external_id
