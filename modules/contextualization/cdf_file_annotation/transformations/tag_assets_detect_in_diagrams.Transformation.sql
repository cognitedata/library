-- =============================================================================
-- TAG ASSETS FOR DIAGRAM DETECT (DetectInDiagrams)
-- =============================================================================
-- Helper transformation to prepare target entities for file annotation launch.
-- Adds DetectInDiagrams to tags without removing existing tag values.
-- Each entity is written back to its own space. An empty targetEntityInstanceSpace
-- tags entities in every space.
--
-- Run manually or on a schedule before the annotation workflow.
-- =============================================================================

SELECT
    a.space
  , a.externalId
  , CASE
      WHEN a.tags IS NULL THEN array('DetectInDiagrams')
      WHEN array_contains(a.tags, 'DetectInDiagrams') THEN a.tags
      ELSE array_union(a.tags, array('DetectInDiagrams'))
    END AS tags
FROM cdf_nodes(
    '{{ targetEntitySchemaSpace }}'
  , '{{ targetEntityExternalId }}'
  , '{{ targetEntityVersion }}'
) AS a
WHERE '{{ targetEntityInstanceSpace }}' = '' OR a.space = '{{ targetEntityInstanceSpace }}'
