SELECT
  cast(key as string) as externalId,
  cast(name as string) as name,
  cast(description as string) as description,
  cast(`order` as string) as `order`,
  cast(routingNo as int) as routingNo,
  CASE WHEN dueDate IS NOT NULL AND dueDate != '' THEN to_timestamp(dueDate, 'yyyy-MM-dd') ELSE NULL END as dueDate,
  cast(mainOrder as string) as mainOrder,
  cast(planningPlant as string) as planningPlant,
  cast(maintPlant as string) as maintPlant,
  cast(plannerGroup as string) as plannerGroup,
  cast(maintActivityType as string) as maintActivityType,
  cast(typeDescription as string) as typeDescription,
  array(cast(systemStatusCode AS STRING)) AS systemStatusCode,
  array(cast(systemStatusDesc AS STRING)) AS systemStatusCodeDesc,
  CASE
    WHEN SPNPriority IS NULL OR TRIM(SPNPriority) = '' THEN NULL
    ELSE cast(NULLIF(regexp_extract(SPNPriority, '^\\s*([0-9]+(?:\\.[0-9]+)?)', 1), '') as double)
  END as SPNPriority,
  CASE WHEN scheduledStartDateTime IS NOT NULL AND scheduledStartDateTime != '' THEN to_timestamp(scheduledStartDateTime) ELSE NULL END as scheduledStartDateTime,
  CASE WHEN scheduledFinishDateTime IS NOT NULL AND scheduledFinishDateTime != '' THEN to_timestamp(scheduledFinishDateTime) ELSE NULL END as scheduledFinishDateTime,
  CASE WHEN actualOrderStartDateTime IS NOT NULL AND actualOrderStartDateTime != '' THEN to_timestamp(actualOrderStartDateTime) ELSE NULL END as actualOrderStartDateTime,
  CASE WHEN actualOrderFinishDateTime IS NOT NULL AND actualOrderFinishDateTime != '' THEN to_timestamp(actualOrderFinishDateTime) ELSE NULL END as actualOrderFinishDateTime,
  CASE WHEN createdDateTime IS NOT NULL AND createdDateTime != '' THEN to_timestamp(createdDateTime) ELSE NULL END as createdDateTime,
  cast(totalPlannedWork as double) as totalPlannedWork,
  cast(totalActualWork as double) as totalActualWork,
  cast(remainingWork as double) as totalRemainingWork,
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
FROM `cfihos_oil_and_gas`.`work_order`
WHERE is_new('work_order', lastUpdatedTime)
