-- Report to calculate percentage of tags identified in time series records that are linked to existing assets
SELECT
  CONCAT(lower(dm_timeSeries.externalId), '_', COALESCE(lower(tag), 'null')) AS key,
  dm_timeSeries.name AS Name,
  dm_timeSeries.externalId AS SourceId,
  'PI' AS SourceName,
  dm_timeSeries.sysUnit AS sysUnit,
  '{{ organization }}' AS sysSite,
  'Asset Link' as TagCategory,
  'PI' AS SourceRecordCategory,
  tag AS TagInSource,
  CASE 
    WHEN array_contains(dm_timeSeries.sysTagsLinked, tag) THEN tag
    ELSE NULL
  END AS TagLinked
FROM
  cdf_data_models(
    "{{ schemaSpace }}",
    "{{ organization }}ProcessIndustries",
    "{{ datamodelVersion }}",
    "{{ organization }}TimeSeries"
  ) dm_timeSeries

LATERAL VIEW explode(
  CASE 
    WHEN size(dm_timeSeries.sysTagsFound) > 0 THEN dm_timeSeries.sysTagsFound
    ELSE ARRAY(NULL)
  END
) exploded_tags AS tag
