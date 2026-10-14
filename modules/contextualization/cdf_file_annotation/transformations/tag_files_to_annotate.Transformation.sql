-- =============================================================================
-- TAG FILES TO ANNOTATE (ToAnnotate)
-- =============================================================================
-- Helper transformation so the prepare function picks up files for annotation.
-- Adds ToAnnotate without removing existing tags (including DetectInDiagrams).
-- Each file is written back to its own space. An empty fileInstanceSpace tags
-- files in every space.
-- =============================================================================

SELECT
    f.space
  , f.externalId
  , CASE
      WHEN f.tags IS NULL THEN array('ToAnnotate')
      WHEN array_contains(f.tags, 'ToAnnotate') THEN f.tags
      ELSE array_union(f.tags, array('ToAnnotate'))
    END AS tags
FROM cdf_nodes(
    '{{ fileSchemaSpace }}'
  , '{{ fileExternalId }}'
  , '{{ fileVersion }}'
) AS f
WHERE '{{ fileInstanceSpace }}' = '' OR f.space = '{{ fileInstanceSpace }}'
