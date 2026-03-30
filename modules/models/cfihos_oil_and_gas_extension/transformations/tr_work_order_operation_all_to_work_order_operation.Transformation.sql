SELECT
  cast(woo.key as string) as externalId,
  cast(woo.name as string) as name,
  cast(woo.description as string) as description,
  cast(woo.operationCounter as string) as operationCounter,
  cast(woo.opWctr as string) as opWctr,
  cast(woo.plannedWork as float) as plannedWork,
  cast(woo.actualWork as float) as actualWork,
  cast(woo.remainingWork as float) as remainingWork,
  cast(woo.operation as string) as operation,
  cast(woo.operationDesc as string) as operationDesc,
  cast(woo.wctrDesc as string) as wctrDesc,
  cast(woo.wctrDiscipline as string) as wctrDiscipline,
  array(cast(woo.systemStatusCode AS STRING)) AS systemStatusCode,
  array(cast(woo.systemStatusDesc AS STRING)) AS systemStatusCodeDesc,
  array(cast(woo.userStatusCode AS STRING)) AS userStatusCode,
  CASE
    WHEN woo.maintenanceOrder_externalId IS NOT NULL AND trim(woo.maintenanceOrder_externalId) != '' THEN
      node_reference('{{ instance_space }}', cast(trim(woo.maintenanceOrder_externalId) as string))
    WHEN wo.key IS NOT NULL AND trim(wo.key) != '' THEN
      node_reference('{{ instance_space }}', cast(trim(wo.key) as string))
    ELSE NULL
  END as maintenanceOrder,
  CASE
    WHEN woo.mainAsset_externalId IS NULL OR trim(woo.mainAsset_externalId) = '' THEN NULL
    ELSE node_reference('{{ instance_space }}', cast(trim(woo.mainAsset_externalId) as string))
  END as mainAsset,
  CASE
    WHEN woo.mainAsset_externalId IS NULL OR trim(woo.mainAsset_externalId) = '' THEN NULL
    ELSE array(node_reference('{{ instance_space }}', cast(trim(woo.mainAsset_externalId) as string)))
  END as assets,
  CASE
    WHEN woo.startTime IS NULL OR woo.startTime = '' THEN NULL
    ELSE to_timestamp(woo.startTime)
  END as startTime,
  CASE
    WHEN woo.endTime IS NULL OR woo.endTime = '' THEN NULL
    ELSE to_timestamp(woo.endTime)
  END as endTime,
  cast(woo.sourceId as string) as sourceId,
  cast(woo.source as string) as sourceContext
FROM `cfihos_oil_and_gas`.`work_order_operation` woo
LEFT JOIN `cfihos_oil_and_gas`.`work_order` wo
  ON trim(woo.mainAsset_externalId) = trim(wo.mainAsset_externalId)
WHERE is_new('work_order_operation', woo.lastUpdatedTime)
