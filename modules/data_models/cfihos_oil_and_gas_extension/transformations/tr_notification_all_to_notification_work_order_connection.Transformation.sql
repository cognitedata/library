SELECT
  cast(m.notificationExternalId as string) as externalId,
  m.asset as asset,
  node_reference('{{ instance_space }}', cast(m.workOrderExternalId as string)) as maintenanceOrder
FROM (
  SELECT
    n.externalId as notificationExternalId,
    n.asset as asset,
    wo.externalId as workOrderExternalId,
    row_number() OVER (
      PARTITION BY n.externalId
      ORDER BY wo.createdDateTime DESC, wo.externalId DESC
    ) as rn
  FROM cdf_nodes('{{ space }}', 'Notification', '{{ dm_version }}') n
  LEFT JOIN cdf_nodes('{{ space }}', 'WorkOrder', '{{ dm_version }}') wo
    ON wo.space = '{{ instance_space }}'
    AND n.asset IS NOT NULL
    AND array_contains(n.asset, wo.mainAsset)
  WHERE n.space = '{{ instance_space }}'
    AND n.maintenanceOrder IS NULL
) m
WHERE m.rn = 1 AND m.workOrderExternalId IS NOT NULL
