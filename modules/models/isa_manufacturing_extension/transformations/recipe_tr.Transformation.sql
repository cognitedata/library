SELECT
  CAST(key AS string) AS externalId,
  CAST(recipe_id AS string) AS recipe_id,
  CAST(recipe_name AS string) AS name,
  CAST(description AS string) AS description,
  CAST(recipe_version AS string) AS recipe_version,
  CAST(recipe_type AS string) AS recipe_type,
  TO_TIMESTAMP(created_date, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS created_date,
  CAST(status AS string) AS status
FROM `ISA_Manufacturing`.`isa_recipe`
