--
-- Report to calculate percentage of tags identified in files that are linked to existing assets
--
SELECT
  CONCAT(lower(dm_operation.externalId), '_', COALESCE(lower(tag), 'null')) AS key,
  dm_operation.name AS Name,
  dm_operation.sourceId AS SourceId,
  'SAP Operations' AS SourceName,
  '1' AS sysUnit,
  '{{ organization }}' AS sysSite,
  'Operations' AS SourceRecordCategory,
  'Asset Link' as TagCategory,
  tag AS TagInSource,
  CASE 
    WHEN array_contains(dm_operation.sysTagsLinked, tag) THEN tag
    ELSE NULL
  END AS TagLinked
FROM
  cdf_data_models(
    "{{ schemaSpace }}",
    "{{ organization }}ProcessIndustries",
    "{{ datamodelVersion }}",
    "{{ organization }}Operation"
  ) dm_operation
LATERAL VIEW explode(
  CASE 
    WHEN size(dm_operation.sysTagsFound) > 0 THEN dm_operation.sysTagsFound
    ELSE ARRAY(NULL)
  END
) exploded_tags AS tag
