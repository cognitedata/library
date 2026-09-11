-- =============================================================================
-- TAG ASSETS FOR DIAGRAM DETECT (DetectInDiagrams)
-- =============================================================================
-- Helper transformation to prepare target entities for file annotation launch.
-- Adds DetectInDiagrams to tags without removing existing tag values.
--
-- Run manually or on a schedule before the annotation workflow.
-- =============================================================================

SELECT
    externalId
  , CASE
      WHEN tags IS NULL THEN array('DetectInDiagrams')
      WHEN array_contains(tags, 'DetectInDiagrams') THEN tags
      ELSE array_union(tags, array('DetectInDiagrams'))
    END AS tags
FROM cdf_nodes(
    '{{ targetEntitySchemaSpace }}'
  , '{{ targetEntityExternalId }}'
  , '{{ targetEntityVersion }}'
)
