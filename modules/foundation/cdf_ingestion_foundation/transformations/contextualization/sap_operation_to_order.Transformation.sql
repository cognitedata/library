-- SAP Notification → WorkOrder relation
-- Sets the Operation.workOrder property linking each notification to its parent
-- work order. Joins workitem (ExNotifheader) to workorder (ExHeaderSet) on OrderId.
-- Verify the join field name `OrderId` against your SAP NW Gateway service definition —
-- in some SAP configurations the notification-to-order link field is named differently
-- (e.g. `OrderNum`, `RefOrderNo`).

WITH unique_notifications AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY `NotifNo` ORDER BY `NotifNo`) AS rn
  FROM `db_{{location}}_sap`.`workitem`
),
unique_workorders AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY `OrderId` ORDER BY `OrderId`) AS rn
  FROM `db_{{location}}_sap`.`workorder`
)
SELECT
  cast(n.`NotifNo`  AS STRING)                                        AS externalId,
  node_reference('{{instanceSpace}}', cast(wo.`OrderId` AS STRING))   AS workOrder
FROM unique_notifications n
JOIN unique_workorders wo
  ON n.`OrderId` = wo.`OrderId`
WHERE isnotnull(n.`NotifNo`)
  AND isnotnull(wo.`OrderId`)
  AND n.rn = 1
  AND wo.rn = 1
