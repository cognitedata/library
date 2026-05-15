SELECT
  cast(t.externalId as string) as externalId,
  t.parent as parent,
  t.type as type,
  CASE
    WHEN flp.externalId IS NULL OR trim(cast(flp.externalId as string)) = '' THEN NULL
    ELSE node_reference('{{ instance_space }}', cast(flp.externalId as string))
  END as FunctionalLocationProperties
FROM cdf_nodes('{{ space }}', 'Tag', '{{ dm_version }}') t
LEFT JOIN cdf_nodes('{{ space }}', 'FunctionalLocationProperties', '{{ dm_version }}') flp
  ON flp.space = '{{ instance_space }}'
  AND flp.externalId = t.externalId
WHERE t.space = '{{ instance_space }}'
