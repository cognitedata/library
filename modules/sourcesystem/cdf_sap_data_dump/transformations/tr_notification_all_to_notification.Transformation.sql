SELECT
  cast(key as string) as externalId,
  cast(name as string) as name,
  cast(description as string) as description,
  cast(mainWorkCenter as string) as mainWorkCenter,
  cast(detectionMethodCode as string) as detectionMethodCode,
  cast(detectionMethodDesc as string) as detectionMethodDesc,
  cast(failureModeCode as string) as failureModeCode,
  cast(failureModeDesc as string) as failureModeDesc,
  cast(failureEffectCode as string) as failureEffectCode,
  cast(failureEffectDesc as string) as failureEffectDesc,
  array(cast(systemStatusCode AS STRING)) AS systemStatusCode,
  array(cast(systemStatusDesc AS STRING)) AS systemStatusCodeDesc,
  cast(notificationPriorityType as string) as notificationPriorityType,
  cast(plannerGroup as string) as plannerGroup,
  CASE
    WHEN dueDate IS NULL OR dueDate = '' THEN NULL
    ELSE to_timestamp(dueDate, 'yyyy-MM-dd')
  END as dueDate,
  cast(locationId as string) as locationId,
  cast(maintPlant as string) as maintPlant,
  cast(catalogType as string) as catalogType,
  cast(catalogGroup as string) as catalogGroup,
  CASE
    WHEN maintenanceOrder_externalId IS NULL OR trim(maintenanceOrder_externalId) = '' THEN NULL
    ELSE node_reference('{{ instance_space }}', cast(trim(maintenanceOrder_externalId) as string))
  END as maintenanceOrder,
  CASE
    WHEN asset_externalId IS NULL OR trim(asset_externalId) = '' THEN NULL
    ELSE array(node_reference('{{ instance_space }}', cast(trim(asset_externalId) as string)))
  END as asset,
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
FROM `cfihos_oil_and_gas`.`notification`
WHERE 1=1 -- full reload: is_new('notification', lastUpdatedTime)
