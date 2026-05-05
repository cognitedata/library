SELECT
  cast(ts.externalId as string) as externalId,
  cast(ts.isStep as boolean) as isStep,
  cast(ts.type as string) as type,
  CASE
    WHEN max(CASE WHEN eq.externalId IS NOT NULL AND eq.externalId != '' THEN 1 ELSE 0 END) = 0 THEN NULL
    ELSE collect_set(node_reference('{{ instance_space }}', cast(eq.externalId as string)))
  END as equipment
FROM cdf_nodes('{{ space }}', 'TimeSeriesData', '{{ dm_version }}') ts
LEFT JOIN cdf_nodes('{{ space }}', 'Equipment', '{{ dm_version }}') eq
  ON eq.space = '{{ instance_space }}'
  AND array_contains(ts.assets, eq.asset)
WHERE ts.space = '{{ instance_space }}'
GROUP BY ts.externalId, ts.isStep, ts.type
