SELECT
  cast(eq.externalId as string) as externalId,
  CASE
    WHEN max(CASE WHEN f.externalId IS NOT NULL AND trim(f.externalId) != '' THEN 1 ELSE 0 END) = 0 THEN NULL
    ELSE collect_set(node_reference('{{ instance_space }}', cast(f.externalId as string)))
  END as files
FROM cdf_nodes('{{ space }}', 'Equipment', '{{ dm_version }}') eq
LEFT JOIN cdf_nodes('{{ space }}', 'Files', '{{ dm_version }}') f
  ON f.space = '{{ instance_space }}'
  AND eq.space = '{{ instance_space }}'
  AND eq.asset IS NOT NULL
  AND f.assets IS NOT NULL
  AND array_contains(f.assets, eq.asset)
GROUP BY eq.externalId
