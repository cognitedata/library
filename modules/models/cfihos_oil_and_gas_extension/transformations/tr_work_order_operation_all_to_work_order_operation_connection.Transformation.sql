SELECT
  cast(woo.externalId as string) as externalId,
  CASE
    WHEN woo.mainAsset IS NOT NULL THEN woo.mainAsset
    WHEN max(CASE WHEN wo.mainAsset IS NOT NULL THEN 1 ELSE 0 END) = 0 THEN NULL
    ELSE node_reference('{{ instance_space }}', cast(min(wo.mainAsset.externalId) as string))
  END as mainAsset,
  CASE
    WHEN woo.assets IS NOT NULL AND size(woo.assets) > 0 THEN woo.assets
    WHEN woo.mainAsset IS NOT NULL THEN array(woo.mainAsset)
    WHEN max(CASE WHEN wo.mainAsset IS NOT NULL THEN 1 ELSE 0 END) = 0 THEN NULL
    ELSE array(node_reference('{{ instance_space }}', cast(min(wo.mainAsset.externalId) as string)))
  END as assets,
  CASE
    WHEN max(CASE WHEN eq.externalId IS NOT NULL AND eq.externalId != '' THEN 1 ELSE 0 END) = 0 THEN NULL
    ELSE collect_set(node_reference('{{ instance_space }}', cast(eq.externalId as string)))
  END as equipment,
  CASE
    WHEN max(CASE WHEN ts.externalId IS NOT NULL AND ts.externalId != '' THEN 1 ELSE 0 END) = 0 THEN NULL
    ELSE collect_set(node_reference('{{ instance_space }}', cast(ts.externalId as string)))
  END as timeSeries,
  CASE
    WHEN woo.maintenanceOrder IS NOT NULL THEN woo.maintenanceOrder
    WHEN max(CASE WHEN wo.externalId IS NOT NULL AND wo.externalId != '' THEN 1 ELSE 0 END) = 0 THEN NULL
    ELSE node_reference('{{ instance_space }}', cast(min(wo.externalId) as string))
  END as maintenanceOrder,
  CASE
    WHEN max(CASE WHEN fl.externalId IS NOT NULL AND fl.externalId != '' THEN 1 ELSE 0 END) = 0 THEN NULL
    ELSE node_reference('{{ instance_space }}', cast(min(fl.externalId) as string))
  END as functionalLocation
FROM cdf_nodes('{{ space }}', 'WorkOrderOperation', '{{ dm_version }}') woo
LEFT JOIN cdf_nodes('{{ space }}', 'Equipment', '{{ dm_version }}') eq
  ON eq.space = '{{ instance_space }}'
  AND woo.mainAsset = eq.asset
LEFT JOIN cdf_nodes('{{ space }}', 'TimeSeriesData', '{{ dm_version }}') ts
  ON ts.space = '{{ instance_space }}'
  AND array_contains(ts.assets, woo.mainAsset)
LEFT JOIN cdf_nodes('{{ space }}', 'WorkOrder', '{{ dm_version }}') wo
  ON wo.space = '{{ instance_space }}'
  AND (
    (woo.maintenanceOrder IS NOT NULL AND wo.externalId = woo.maintenanceOrder.externalId)
    OR (woo.mainAsset IS NOT NULL AND woo.mainAsset = wo.mainAsset)
  )
LEFT JOIN cdf_nodes('{{ space }}', 'FunctionalLocation', '{{ dm_version }}') fl
  ON fl.space = '{{ instance_space }}'
  AND woo.mainAsset.externalId = fl.flocMainAsset
WHERE woo.space = '{{ instance_space }}'
GROUP BY woo.externalId, woo.mainAsset, woo.assets, woo.maintenanceOrder
