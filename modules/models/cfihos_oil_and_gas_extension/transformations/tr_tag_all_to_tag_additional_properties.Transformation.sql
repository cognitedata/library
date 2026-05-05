SELECT
  cast(t.externalId as string) as externalId,
  t.parent as parent,
  t.type as type,
  to_json(named_struct(
    'commonLCIProperties',
    CASE WHEN clp.externalId IS NULL THEN NULL ELSE struct(clp.*) END,
    'functionalLocationProperties',
    CASE WHEN flp.externalId IS NULL THEN NULL ELSE struct(flp.*) END,
    'classSpecificView',
    CASE
      WHEN comp.externalId IS NOT NULL THEN 'Compressor'
      WHEN drill.externalId IS NOT NULL THEN 'DrillingEquipment'
      WHEN elec.externalId IS NOT NULL THEN 'ElectricalEquipmentClass'
      WHEN encl.externalId IS NOT NULL THEN 'Enclosure'
      WHEN hse.externalId IS NOT NULL THEN 'HealthSafetyAndEnvironmentEquipmentClass'
      WHEN heat.externalId IS NOT NULL THEN 'HeatExchanger'
      WHEN infra.externalId IS NOT NULL THEN 'Infrastructure'
      WHEN inst.externalId IS NOT NULL THEN 'InstrumentEquipment'
      WHEN itel.externalId IS NOT NULL THEN 'ItAndTelecomEquipment'
      WHEN mech.externalId IS NOT NULL THEN 'MechanicalEquipmentClass'
      WHEN misc.externalId IS NOT NULL THEN 'MiscellaneousEquipment'
      WHEN pipe.externalId IS NOT NULL THEN 'PipingAndPipelineEquipment'
      WHEN pump.externalId IS NOT NULL THEN 'Pump'
      WHEN subsea.externalId IS NOT NULL THEN 'SubseaEquipmentClass'
      WHEN tool.externalId IS NOT NULL THEN 'Tool'
      WHEN turb.externalId IS NOT NULL THEN 'Turbine'
      WHEN valve.externalId IS NOT NULL THEN 'Valve'
      ELSE NULL
    END,
    'classSpecificProperties',
    coalesce(
      CASE WHEN comp.externalId IS NOT NULL THEN to_json(struct(comp.*)) END,
      CASE WHEN drill.externalId IS NOT NULL THEN to_json(struct(drill.*)) END,
      CASE WHEN elec.externalId IS NOT NULL THEN to_json(struct(elec.*)) END,
      CASE WHEN encl.externalId IS NOT NULL THEN to_json(struct(encl.*)) END,
      CASE WHEN hse.externalId IS NOT NULL THEN to_json(struct(hse.*)) END,
      CASE WHEN heat.externalId IS NOT NULL THEN to_json(struct(heat.*)) END,
      CASE WHEN infra.externalId IS NOT NULL THEN to_json(struct(infra.*)) END,
      CASE WHEN inst.externalId IS NOT NULL THEN to_json(struct(inst.*)) END,
      CASE WHEN itel.externalId IS NOT NULL THEN to_json(struct(itel.*)) END,
      CASE WHEN mech.externalId IS NOT NULL THEN to_json(struct(mech.*)) END,
      CASE WHEN misc.externalId IS NOT NULL THEN to_json(struct(misc.*)) END,
      CASE WHEN pipe.externalId IS NOT NULL THEN to_json(struct(pipe.*)) END,
      CASE WHEN pump.externalId IS NOT NULL THEN to_json(struct(pump.*)) END,
      CASE WHEN subsea.externalId IS NOT NULL THEN to_json(struct(subsea.*)) END,
      CASE WHEN tool.externalId IS NOT NULL THEN to_json(struct(tool.*)) END,
      CASE WHEN turb.externalId IS NOT NULL THEN to_json(struct(turb.*)) END,
      CASE WHEN valve.externalId IS NOT NULL THEN to_json(struct(valve.*)) END
    )
  )) as additionalProperties
FROM cdf_nodes('{{ space }}', 'Tag', '{{ dm_version }}') t
LEFT JOIN cdf_nodes('{{ space }}', 'CommonLCIProperties', '{{ dm_version }}') clp
  ON clp.space = '{{ instance_space }}'
  AND clp.externalId = t.externalId
LEFT JOIN cdf_nodes('{{ space }}', 'FunctionalLocationProperties', '{{ dm_version }}') flp
  ON flp.space = '{{ instance_space }}'
  AND flp.externalId = t.externalId
LEFT JOIN cdf_nodes('{{ space }}', 'Compressor', '{{ dm_version }}') comp
  ON comp.space = '{{ instance_space }}'
  AND comp.externalId = coalesce(t.classSpecificProperties.externalId, t.externalId)
LEFT JOIN cdf_nodes('{{ space }}', 'DrillingEquipment', '{{ dm_version }}') drill
  ON drill.space = '{{ instance_space }}'
  AND drill.externalId = coalesce(t.classSpecificProperties.externalId, t.externalId)
LEFT JOIN cdf_nodes('{{ space }}', 'ElectricalEquipmentClass', '{{ dm_version }}') elec
  ON elec.space = '{{ instance_space }}'
  AND elec.externalId = coalesce(t.classSpecificProperties.externalId, t.externalId)
LEFT JOIN cdf_nodes('{{ space }}', 'Enclosure', '{{ dm_version }}') encl
  ON encl.space = '{{ instance_space }}'
  AND encl.externalId = coalesce(t.classSpecificProperties.externalId, t.externalId)
LEFT JOIN cdf_nodes('{{ space }}', 'HealthSafetyAndEnvironmentEquipmentClass', '{{ dm_version }}') hse
  ON hse.space = '{{ instance_space }}'
  AND hse.externalId = coalesce(t.classSpecificProperties.externalId, t.externalId)
LEFT JOIN cdf_nodes('{{ space }}', 'HeatExchanger', '{{ dm_version }}') heat
  ON heat.space = '{{ instance_space }}'
  AND heat.externalId = coalesce(t.classSpecificProperties.externalId, t.externalId)
LEFT JOIN cdf_nodes('{{ space }}', 'Infrastructure', '{{ dm_version }}') infra
  ON infra.space = '{{ instance_space }}'
  AND infra.externalId = coalesce(t.classSpecificProperties.externalId, t.externalId)
LEFT JOIN cdf_nodes('{{ space }}', 'InstrumentEquipment', '{{ dm_version }}') inst
  ON inst.space = '{{ instance_space }}'
  AND inst.externalId = coalesce(t.classSpecificProperties.externalId, t.externalId)
LEFT JOIN cdf_nodes('{{ space }}', 'ItAndTelecomEquipment', '{{ dm_version }}') itel
  ON itel.space = '{{ instance_space }}'
  AND itel.externalId = coalesce(t.classSpecificProperties.externalId, t.externalId)
LEFT JOIN cdf_nodes('{{ space }}', 'MechanicalEquipmentClass', '{{ dm_version }}') mech
  ON mech.space = '{{ instance_space }}'
  AND mech.externalId = coalesce(t.classSpecificProperties.externalId, t.externalId)
LEFT JOIN cdf_nodes('{{ space }}', 'MiscellaneousEquipment', '{{ dm_version }}') misc
  ON misc.space = '{{ instance_space }}'
  AND misc.externalId = coalesce(t.classSpecificProperties.externalId, t.externalId)
LEFT JOIN cdf_nodes('{{ space }}', 'PipingAndPipelineEquipment', '{{ dm_version }}') pipe
  ON pipe.space = '{{ instance_space }}'
  AND pipe.externalId = coalesce(t.classSpecificProperties.externalId, t.externalId)
LEFT JOIN cdf_nodes('{{ space }}', 'Pump', '{{ dm_version }}') pump
  ON pump.space = '{{ instance_space }}'
  AND pump.externalId = coalesce(t.classSpecificProperties.externalId, t.externalId)
LEFT JOIN cdf_nodes('{{ space }}', 'SubseaEquipmentClass', '{{ dm_version }}') subsea
  ON subsea.space = '{{ instance_space }}'
  AND subsea.externalId = coalesce(t.classSpecificProperties.externalId, t.externalId)
LEFT JOIN cdf_nodes('{{ space }}', 'Tool', '{{ dm_version }}') tool
  ON tool.space = '{{ instance_space }}'
  AND tool.externalId = coalesce(t.classSpecificProperties.externalId, t.externalId)
LEFT JOIN cdf_nodes('{{ space }}', 'Turbine', '{{ dm_version }}') turb
  ON turb.space = '{{ instance_space }}'
  AND turb.externalId = coalesce(t.classSpecificProperties.externalId, t.externalId)
LEFT JOIN cdf_nodes('{{ space }}', 'Valve', '{{ dm_version }}') valve
  ON valve.space = '{{ instance_space }}'
  AND valve.externalId = coalesce(t.classSpecificProperties.externalId, t.externalId)
WHERE t.space = '{{ instance_space }}'
  AND (
    clp.externalId IS NOT NULL
    OR flp.externalId IS NOT NULL
    OR comp.externalId IS NOT NULL
    OR drill.externalId IS NOT NULL
    OR elec.externalId IS NOT NULL
    OR encl.externalId IS NOT NULL
    OR hse.externalId IS NOT NULL
    OR heat.externalId IS NOT NULL
    OR infra.externalId IS NOT NULL
    OR inst.externalId IS NOT NULL
    OR itel.externalId IS NOT NULL
    OR mech.externalId IS NOT NULL
    OR misc.externalId IS NOT NULL
    OR pipe.externalId IS NOT NULL
    OR pump.externalId IS NOT NULL
    OR subsea.externalId IS NOT NULL
    OR tool.externalId IS NOT NULL
    OR turb.externalId IS NOT NULL
    OR valve.externalId IS NOT NULL
  )
