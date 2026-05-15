SELECT DISTINCT
  CONCAT('dwg_', COALESCE('{{ organization }}', ''), '_', COALESCE('1', ''), '_', COALESCE(tags.startNode, ''), '_', COALESCE(tags.startNodeText, '')) AS key,
  tags.startNode AS Name,
  '1' AS sysUnit,
  '{{ organization }}' AS sysSite,
  'Files' AS SourceName,
  tags.startNode AS SourceId,
  'Asset Annotation' AS TagCategory,
  dm_file.category AS SourceRecordCategory,
  tags.startNodeText AS TagLinked,
  NULL AS TagInSource

FROM {{ annotation_db }}.{{ annotation_docs_tbl}} AS tags

-- Join baseline to get sysSite and sysUnit for this sourceId
JOIN {{ annotation_db }}.{{ annotation_patterns_tbl }} AS base
  ON base.startNode = tags.startNode
  AND base.endNodeResourceType = 'Asset'

-- Ensure tag is not in baseline
LEFT JOIN {{ annotation_db }}.{{ annotation_patterns_tbl }} AS check_tag
  ON check_tag.startNode = tags.startNode
     AND check_tag.startNodeText = tags.startNodeText

-- Join dm_fileFile to get fileCategory
LEFT JOIN cdf_data_models(
    "{{ schemaSpace }}",
    "{{ datamodelExternalId }}",
    "{{ datamodelVersion }}",
    "FileRevision"  
) AS dm_file
  ON dm_file.sourceId = tags.startNode

WHERE 
  check_tag.startNodeText IS NULL  -- tag not in baseline

