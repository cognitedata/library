WITH base AS (
  SELECT
    CAST(workOrderId AS string) AS workOrderExternalId,
    CAST(workOrderId AS string) AS workOrderId,
    CAST(workOrderNumber AS string) AS workOrderNumber,
    CAST(workOrderName AS string) AS workOrderName,
    CAST(description AS string) AS description,
    CAST(workType AS string) AS workType,
    CAST(workStatus AS string) AS workStatus,
    assetExternalId,
    CASE
      WHEN plannedStartTime IS NULL OR plannedStartTime = '' THEN NULL
      ELSE TO_TIMESTAMP(plannedStartTime, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    END AS plannedStartTimeTs,
    CASE
      WHEN plannedEndTime IS NULL OR plannedEndTime = '' THEN NULL
      ELSE TO_TIMESTAMP(plannedEndTime, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    END AS plannedEndTimeTs,
    CASE
      WHEN actualStartTime IS NULL OR actualStartTime = '' THEN NULL
      ELSE TO_TIMESTAMP(actualStartTime, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    END AS actualStartTimeTs,
    CASE
      WHEN actualEndTime IS NULL OR actualEndTime = '' THEN NULL
      ELSE TO_TIMESTAMP(actualEndTime, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    END AS actualEndTimeTs,
    CASE
      WHEN equipmentExternalId IS NULL OR equipmentExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', CAST(equipmentExternalId AS string))
    END AS assignedEquipmentRef,
    CASE
      WHEN assetExternalId IS NULL OR assetExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', CAST(assetExternalId AS string))
    END AS assetRef,
    CASE
      WHEN assignedPersonnelExternalId IS NULL OR assignedPersonnelExternalId = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', CAST(assignedPersonnelExternalId AS string))
    END AS assignedPersonnelRef
  FROM `{{ rawDatabase }}`.`isa_work_order`
  WHERE 1=1 -- full reload: is_new('isa_work_order', lastUpdatedTime)
)

SELECT
  workOrderExternalId AS externalId,
  FIRST(workOrderId, true) AS workOrderId,
  FIRST(workOrderNumber, true) AS workOrderNumber,
  FIRST(workOrderName, true) AS name,
  FIRST(description, true) AS description,
  FIRST(workType, true) AS workType,
  FIRST(workStatus, true) AS workStatus,
  FIRST(plannedStartTimeTs, true) AS plannedStartTime,
  FIRST(plannedEndTimeTs, true) AS plannedEndTime,
  FIRST(actualStartTimeTs, true) AS actualStartTime,
  FIRST(actualEndTimeTs, true) AS actualEndTime,
  FIRST(actualStartTimeTs, true) AS startTime,
  FIRST(actualEndTimeTs, true) AS endTime,
  FIRST(plannedStartTimeTs, true) AS scheduledStartTime,
  FIRST(plannedEndTimeTs, true) AS scheduledEndTime,
  FILTER(COLLECT_SET(assignedEquipmentRef), x -> x IS NOT NULL) AS equipment,
  FILTER(COLLECT_SET(assetRef), x -> x IS NOT NULL) AS assets,
  FILTER(COLLECT_SET(assignedPersonnelRef), x -> x IS NOT NULL) AS assignedPersonnel
FROM base
GROUP BY workOrderExternalId
