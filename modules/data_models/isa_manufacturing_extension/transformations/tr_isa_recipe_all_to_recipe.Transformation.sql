WITH base AS (
  SELECT
    CAST(key AS string) AS external_id,
    CAST(recipeId AS string) AS recipeId,
    CAST(recipeName AS string) AS recipeName,
    CAST(description AS string) AS description,
    CAST(recipeVersion AS string) AS recipeVersion,
    CAST(recipeType AS string) AS recipeType,
    CASE
      WHEN createdDate IS NULL OR createdDate = '' THEN NULL
      ELSE TO_TIMESTAMP(createdDate, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    END AS createdDate,
    CAST(status AS string) AS status
  FROM `{{ rawDatabase }}`.`isa_recipe`
  WHERE 1=1 -- full reload: is_new('isa_recipe', lastUpdatedTime)
)
SELECT
  external_id AS externalId,
  FIRST(recipeId, true) AS recipeId,
  FIRST(recipeName, true) AS name,
  FIRST(description, true) AS description,
  FIRST(recipeVersion, true) AS recipeVersion,
  FIRST(recipeType, true) AS recipeType,
  FIRST(createdDate, true) AS createdDate,
  FIRST(status, true) AS status
FROM base
GROUP BY external_id
