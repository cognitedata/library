SELECT
  CAST(material_id AS string) AS externalId,
  CAST(material_id AS string) AS material_id,
  CAST(material_name AS string) AS name,
  CAST(material_class AS string) AS material_class,
  CAST(description AS string) AS description,
  CASE
    WHEN primary_recipe_externalId IS NULL OR primary_recipe_externalId = '' THEN NULL
    ELSE ARRAY(node_reference('{{ isaInstanceSpace }}', CAST(primary_recipe_externalId AS string)))
  END AS recipe,
  CASE
    WHEN batch_externalId IS NULL OR batch_externalId = '' THEN NULL
    ELSE ARRAY(node_reference('{{ isaInstanceSpace }}', CAST(batch_externalId AS string)))
  END AS batch
FROM `ISA_Manufacturing`.`isa_material`
