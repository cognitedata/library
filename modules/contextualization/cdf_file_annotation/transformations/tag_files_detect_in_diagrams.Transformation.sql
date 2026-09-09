-- =============================================================================
-- TAG FILES FOR DIAGRAM DETECT (DetectInDiagrams)
-- =============================================================================
-- Helper transformation so file instances can be used as cross-reference entities
-- in diagram detect. Adds DetectInDiagrams without removing existing tags.
-- =============================================================================

SELECT
    externalId
  , CASE
      WHEN tags IS NULL THEN array('DetectInDiagrams')
      WHEN array_contains(tags, 'DetectInDiagrams') THEN tags
      ELSE array_union(tags, array('DetectInDiagrams'))
    END AS tags
FROM cdf_data_models(
    '{{ fileSchemaSpace }}'
  , '{{ cdmDataModelExternalId }}'
  , '{{ fileVersion }}'
  , '{{ fileExternalId }}'
)
WHERE space = '{{ fileInstanceSpace }}'
