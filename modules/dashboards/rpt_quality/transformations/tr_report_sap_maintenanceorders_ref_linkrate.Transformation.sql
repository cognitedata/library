--
-- Report to calculate percentage of tags identified in files that are linked to existing assets
--
SELECT
  CONCAT(lower(dm_maintenanceorder.externalId), '_', COALESCE(lower(tag), 'null')) AS key,
  dm_maintenanceorder.name AS Name,
  dm_maintenanceorder.sourceId AS SourceId,
  'SAP Maintenance Orders' AS SourceName,
  '1' AS sysUnit,
  '{{ organization }}' AS sysSite,
  'Maintenance Order' AS SourceRecordCategory,
  'Asset Link' as TagCategory,
  tag AS TagInSource,
  CASE 
    WHEN array_contains(dm_maintenanceorder.sysTagsLinked, tag) THEN tag
    ELSE NULL
  END AS TagLinked
FROM
  cdf_data_models(
    "{{ schemaSpace }}",
    "{{ datamodelExternalId }}",
    "{{ datamodelVersion }}",
    "MaintenanceOrder"
  ) dm_maintenanceorder
LATERAL VIEW explode(
  CASE 
    WHEN size(dm_maintenanceorder.sysTagsFound) > 0 THEN dm_maintenanceorder.sysTagsFound
    ELSE ARRAY(NULL)
  END
) exploded_tags AS tag
