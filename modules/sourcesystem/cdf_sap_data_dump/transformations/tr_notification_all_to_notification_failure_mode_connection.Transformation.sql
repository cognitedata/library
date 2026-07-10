SELECT
  cast(n.externalId as string) as externalId,
  n.asset as asset,
  n.maintenanceOrder as maintenanceOrder,
  CASE
    WHEN max(CASE WHEN fm.externalId IS NOT NULL AND trim(cast(fm.externalId as string)) != '' THEN 1 ELSE 0 END) = 0 THEN NULL
    ELSE node_reference('{{ instance_space }}', cast(min(fm.externalId) as string))
  END as failureMode
FROM cdf_nodes('{{ space }}', 'Notification', '{{ dm_version }}') n
LEFT JOIN cdf_nodes('{{ space }}', 'FailureMode', '{{ dm_version }}') fm
  ON fm.space = '{{ instance_space }}'
  AND n.failureModeCode IS NOT NULL
  AND trim(cast(n.failureModeCode as string)) != ''
  AND lower(trim(cast(n.failureModeCode as string))) = lower(trim(cast(fm.code as string)))
WHERE n.space = '{{ instance_space }}'
GROUP BY n.externalId, n.asset, n.maintenanceOrder
