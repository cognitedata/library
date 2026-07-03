SELECT
  cast(t.externalId as string) as externalId,
  t.parent as parent,
  t.type as type,
  CASE
    WHEN clp.externalId IS NULL OR trim(cast(clp.externalId as string)) = '' THEN NULL
    ELSE node_reference('{{ instance_space }}', cast(clp.externalId as string))
  END as CommonLCIProperties
FROM cdf_nodes('{{ space }}', 'Tag', '{{ dm_version }}') t
LEFT JOIN cdf_nodes('{{ space }}', 'CommonLCIProperties', '{{ dm_version }}') clp
  ON clp.space = '{{ instance_space }}'
  AND clp.externalId = t.externalId
WHERE t.space = '{{ instance_space }}'
