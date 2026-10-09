-- =============================================================================
-- TAG FILES FOR DIAGRAM DETECT (DetectInDiagrams)
-- =============================================================================
-- Helper transformation so file instances can be used as cross-reference entities
-- in diagram detect. Adds DetectInDiagrams without removing existing tags.
-- Each file is written back to its own space. An empty fileInstanceSpace tags
-- files in every space.
-- =============================================================================

SELECT
    f.space
  , f.externalId
  , CASE
      WHEN f.tags IS NULL THEN array('DetectInDiagrams')
      WHEN array_contains(f.tags, 'DetectInDiagrams') THEN f.tags
      ELSE array_union(f.tags, array('DetectInDiagrams'))
    END AS tags
FROM cdf_nodes(
    '{{ fileSchemaSpace }}'
  , '{{ fileExternalId }}'
  , '{{ fileVersion }}'
) AS f
WHERE '{{ fileInstanceSpace }}' = '' OR f.space = '{{ fileInstanceSpace }}'
