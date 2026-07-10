SELECT
  cast(t.externalId as string) as externalId,
  t.parent as parent,
  t.type as type,
  CASE
    WHEN max(CASE WHEN cs.externalId IS NOT NULL AND trim(cast(cs.externalId as string)) != '' THEN 1 ELSE 0 END) = 0 THEN NULL
    ELSE node_reference('{{ instance_space }}', cast(min(cs.externalId) as string))
  END as classSpecific
FROM cdf_nodes('{{ space }}', 'Tag', '{{ dm_version }}') t
LEFT JOIN (
  SELECT externalId, space FROM cdf_nodes('{{ space }}', 'Compressor', '{{ dm_version }}')
  UNION ALL
  SELECT externalId, space FROM cdf_nodes('{{ space }}', 'DrillingEquipment', '{{ dm_version }}')
  UNION ALL
  SELECT externalId, space FROM cdf_nodes('{{ space }}', 'ElectricalEquipmentClass', '{{ dm_version }}')
  UNION ALL
  SELECT externalId, space FROM cdf_nodes('{{ space }}', 'Enclosure', '{{ dm_version }}')
  UNION ALL
  SELECT externalId, space FROM cdf_nodes('{{ space }}', 'HealthSafetyAndEnvironmentEquipmentClass', '{{ dm_version }}')
  UNION ALL
  SELECT externalId, space FROM cdf_nodes('{{ space }}', 'HeatExchanger', '{{ dm_version }}')
  UNION ALL
  SELECT externalId, space FROM cdf_nodes('{{ space }}', 'Infrastructure', '{{ dm_version }}')
  UNION ALL
  SELECT externalId, space FROM cdf_nodes('{{ space }}', 'InstrumentEquipment', '{{ dm_version }}')
  UNION ALL
  SELECT externalId, space FROM cdf_nodes('{{ space }}', 'ItAndTelecomEquipment', '{{ dm_version }}')
  UNION ALL
  SELECT externalId, space FROM cdf_nodes('{{ space }}', 'MechanicalEquipmentClass', '{{ dm_version }}')
  UNION ALL
  SELECT externalId, space FROM cdf_nodes('{{ space }}', 'MiscellaneousEquipment', '{{ dm_version }}')
  UNION ALL
  SELECT externalId, space FROM cdf_nodes('{{ space }}', 'PipingAndPipelineEquipment', '{{ dm_version }}')
  UNION ALL
  SELECT externalId, space FROM cdf_nodes('{{ space }}', 'Pump', '{{ dm_version }}')
  UNION ALL
  SELECT externalId, space FROM cdf_nodes('{{ space }}', 'SubseaEquipmentClass', '{{ dm_version }}')
  UNION ALL
  SELECT externalId, space FROM cdf_nodes('{{ space }}', 'Tool', '{{ dm_version }}')
  UNION ALL
  SELECT externalId, space FROM cdf_nodes('{{ space }}', 'Turbine', '{{ dm_version }}')
  UNION ALL
  SELECT externalId, space FROM cdf_nodes('{{ space }}', 'Valve', '{{ dm_version }}')
) cs
  ON cs.space = '{{ instance_space }}'
  AND cs.externalId = t.externalId
WHERE t.space = '{{ instance_space }}'
GROUP BY t.externalId, t.parent, t.type
