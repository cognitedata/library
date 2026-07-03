SELECT
  cast(ann.startNodeExternalId as string) as externalId,
  CASE
    WHEN max(CASE WHEN ann.endNodeExternalId IS NOT NULL AND trim(ann.endNodeExternalId) != '' THEN 1 ELSE 0 END) = 0 THEN NULL
    ELSE collect_set(node_reference('{{ instance_space }}', cast(ann.endNodeExternalId as string)))
  END as assets
FROM `cfihos_oil_and_gas`.`diagram_annotation` ann
WHERE lower(cast(ann.instanceType as string)) = 'edge'
  AND cast(ann.type as string) = 'diagrams.AssetLink'
  AND cast(ann.startNodeSpace as string) = '{{ instance_space }}'
  AND cast(ann.endNodeSpace as string) = '{{ instance_space }}'
GROUP BY ann.startNodeExternalId
