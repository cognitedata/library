SELECT
  cast(key as string) as externalId,
  cast(instanceType as string) as instanceType,
  cast(space as string) as space,
  node_reference(cast(startNodeSpace as string), cast(startNodeExternalId as string)) as startNode,
  node_reference(cast(endNodeSpace as string), cast(endNodeExternalId as string)) as endNode,
  type_reference('cdf_cdm', cast(type as string)) as type,
  cast(status as string) as status,
  cast(confidence as double) as confidence,
  cast(name as string) as name,
  cast(startNodeText as string) as startNodeText,
  cast(startNodeXMax as double) as startNodeXMax,
  cast(startNodeXMin as double) as startNodeXMin,
  cast(startNodeYMax as double) as startNodeYMax,
  cast(startNodeYMin as double) as startNodeYMin,
  cast(startNodePageNumber as int) as startNodePageNumber,
  CASE
    WHEN sourceCreatedTime IS NULL OR trim(sourceCreatedTime) = '' THEN NULL
    ELSE to_timestamp(sourceCreatedTime)
  END as sourceCreatedTime,
  cast(sourceCreatedUser as string) as sourceCreatedUser,
  CASE
    WHEN sourceUpdatedTime IS NULL OR trim(sourceUpdatedTime) = '' THEN NULL
    ELSE to_timestamp(sourceUpdatedTime)
  END as sourceUpdatedTime,
  cast(sourceUpdatedUser as string) as sourceUpdatedUser
FROM `cfihos_oil_and_gas`.`diagram_annotation`
WHERE lower(cast(instanceType as string)) = 'edge'
