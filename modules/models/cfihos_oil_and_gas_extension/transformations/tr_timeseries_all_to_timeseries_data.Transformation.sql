SELECT
  cast(key as string) as externalId,
  cast(name as string) as name,
  cast(description as string) as description,
  cast(timeSeriesType as string) as timeSeriesType,
  cast(assetName as string) as assetName,
  cast(facility as string) as facility,
  cast(assetDescription as string) as assetDescription,
  CASE WHEN lower(isString) = 'true' THEN 'string' ELSE 'numeric' END as `type`,
  CASE WHEN lower(isString) = 'true' THEN true ELSE false END as isString,
  CASE WHEN lower(isStep) = 'true' THEN true ELSE false END as isStep,
  cast(pi_tag as string) as pi_tag,
  cast(pi_pointId as string) as pi_pointId,
  cast(pi_sourceTag as string) as pi_sourceTag,
  cast(pi_pointSource as string) as pi_pointSource,
  CASE WHEN pi_archiving IS NOT NULL AND pi_archiving != '' THEN cast(pi_archiving as int) ELSE NULL END as pi_archiving,
  CASE
    WHEN asset_externalId IS NULL OR asset_externalId = '' THEN NULL
    ELSE array(node_reference('{{ instance_space }}', cast(asset_externalId as string)))
  END as assets,
  cast(sourceId as string) as sourceId,
  cast(source as string) as sourceContext
FROM `cfihos_oil_and_gas`.`timeseries`
WHERE is_new('timeseries', lastUpdatedTime)
