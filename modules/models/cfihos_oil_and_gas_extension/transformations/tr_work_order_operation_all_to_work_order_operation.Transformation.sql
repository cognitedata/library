SELECT
  cast(key as string) as externalId,
  cast(name as string) as name,
  cast(description as string) as description,
  cast(operationCounter as string) as operationCounter,
  cast(opWctr as string) as opWctr,
  cast(plannedWork as float) as plannedWork,
  cast(actualWork as float) as actualWork,
  cast(remainingWork as float) as remainingWork,
  cast(operation as string) as operation,
  cast(operationDesc as string) as operationDesc,
  cast(wctrDesc as string) as wctrDesc,
  cast(wctrDiscipline as string) as wctrDiscipline,
  array(cast(systemStatusCode AS STRING)) AS systemStatusCode,
  array(cast(systemStatusDesc AS STRING)) AS systemStatusCodeDesc,
  array(cast(userStatusCode AS STRING)) AS userStatusCode,
  CASE
    WHEN maintenanceOrder_externalId IS NULL OR maintenanceOrder_externalId = '' THEN NULL
    ELSE node_reference('{{ instance_space }}', cast(maintenanceOrder_externalId as string))
  END as maintenanceOrder,
  CASE
    WHEN mainAsset_externalId IS NULL OR mainAsset_externalId = '' THEN NULL
    ELSE node_reference('{{ instance_space }}', cast(mainAsset_externalId as string))
  END as mainAsset,
  CASE
    WHEN mainAsset_externalId IS NULL OR mainAsset_externalId = '' THEN NULL
    ELSE array(node_reference('{{ instance_space }}', cast(mainAsset_externalId as string)))
  END as assets,
  CASE
    WHEN startTime IS NULL OR startTime = '' THEN NULL
    ELSE to_timestamp(startTime)
  END as startTime,
  CASE
    WHEN endTime IS NULL OR endTime = '' THEN NULL
    ELSE to_timestamp(endTime)
  END as endTime,
  cast(sourceId as string) as sourceId,
  cast(source as string) as sourceContext
FROM `cfihos_oil_and_gas`.`work_order_operation`
WHERE is_new('work_order_operation', lastUpdatedTime)
