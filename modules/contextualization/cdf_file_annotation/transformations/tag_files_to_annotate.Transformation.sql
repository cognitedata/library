-- =============================================================================
-- TAG FILES TO ANNOTATE (ToAnnotate)
-- =============================================================================
-- Helper transformation so the prepare function picks up files for annotation.
-- Adds ToAnnotate without removing existing tags (including DetectInDiagrams).
-- =============================================================================

SELECT
    externalId
  , CASE
      WHEN tags IS NULL THEN array('ToAnnotate')
      WHEN array_contains(tags, 'ToAnnotate') THEN tags
      ELSE array_union(tags, array('ToAnnotate'))
    END AS tags
FROM cdf_nodes(
    '{{ fileSchemaSpace }}'
  , '{{ fileExternalId }}'
  , '{{ fileVersion }}'
)
