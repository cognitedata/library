-- Updated report to generate report lines for DWG diagram annotation based on
-- baseline and files and doc tags found during annotation

SELECT
  CONCAT('dwg_', COALESCE('{{ organization }}', ''), '_', COALESCE('1', ''), '_', COALESCE(base.startNode, ''), '_', COALESCE(base.startNodeText, '')) AS key,
  base.startNode AS Name,
  '1' AS sysUnit,
  '{{ organization }}' AS sysSite,
  'Files' AS SourceName,
  base.startNode AS SourceId,
  CASE 
    WHEN base.endNodeResourceType = 'Asset' THEN 'Asset Annotation'
    ELSE base.endNodeResourceType
  END AS TagCategory,
  base.startSourceId AS SourceRecordCategory,
  base.startNodeText AS TagInSource,
  CASE 
    WHEN docs.startNode IS NOT NULL OR tags.startNode IS NOT NULL 
         THEN base.startNodeText
    ELSE NULL
  END AS TagLinked
FROM {{ annotation_db }}.{{ annotation_patterns_tbl }} AS base
LEFT OUTER JOIN {{ annotation_db }}.{{ annotation_docs_tbl }} AS docs
  ON base.startNode = docs.startNode AND STARTSWITH(docs.startNodeText, base.startNodeText)

LEFT OUTER JOIN {{ annotation_db }}.{{ annotation_tags_tbl}} AS tags
  ON base.startNode = tags.startNode AND tags.startNodeText = base.startNodeText
