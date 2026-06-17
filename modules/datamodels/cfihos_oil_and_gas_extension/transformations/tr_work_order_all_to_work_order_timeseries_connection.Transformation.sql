SELECT
  cast(wo.externalId as string) as externalId,
  wo.mainAsset as mainAsset,
  wo.assets as assets,
  CASE
    WHEN max(CASE WHEN ts.externalId IS NOT NULL AND ts.externalId != '' THEN 1 ELSE 0 END) = 0 THEN NULL
    ELSE collect_set(node_reference('{{ instance_space }}', cast(ts.externalId as string)))
  END as timeSeries
FROM cdf_nodes('{{ space }}', 'WorkOrder', '{{ dm_version }}') wo
LEFT JOIN cdf_nodes('{{ space }}', 'TimeSeriesData', '{{ dm_version }}') ts
  ON ts.space = '{{ instance_space }}'
  AND array_contains(ts.assets, wo.mainAsset)
WHERE wo.space = '{{ instance_space }}'
GROUP BY wo.externalId, wo.mainAsset, wo.assets
