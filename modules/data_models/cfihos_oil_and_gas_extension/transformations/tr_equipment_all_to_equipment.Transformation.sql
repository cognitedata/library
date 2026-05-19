SELECT
  cast(key as string) as externalId,
  cast(name as string) as name,
  cast(description as string) as description,
  cast(model as string) as model,
  cast(`class` as string) as `class`,
  cast(`type` as string) as `type`,
  cast(code as string) as code,
  cast(equipmentClass as string) as equipmentClass,
  cast(standard as string) as standard,
  cast(standardReference as string) as standardReference,
  CASE
    WHEN asset_externalId IS NULL OR asset_externalId = '' THEN NULL
    ELSE node_reference('{{ instance_space }}', cast(asset_externalId as string))
  END as asset,
  cast(sourceId as string) as sourceId,
  cast(source as string) as sourceContext
FROM `cfihos_oil_and_gas`.`equipment`
WHERE 1=1 -- full reload: is_new('equipment', lastUpdatedTime)
