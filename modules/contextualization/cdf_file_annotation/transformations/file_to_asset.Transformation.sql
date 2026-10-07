-- =============================================================================
-- FILE TO ASSET TRANSFORMATION
-- =============================================================================
-- This transformation adds approved diagram annotations to the 'assets' property
-- on File instances. Assets already on the file are kept.
--
-- Data Sources:
--   - RAW table: annotation_documents_tags (approved asset annotations)
--   - Destination: File view
--
-- Logic:
--   1. Query the asset annotations RAW table for approved annotations
--   2. Group by file (space and external ID), with each asset in its own space
--   3. Merge with the file's existing assets
--   4. Update existing File instances only, in their own space
-- =============================================================================

WITH approved AS (
  SELECT
    startNodeSpace AS space,
    startNode AS externalId,
    collect_set(node_reference(endNodeSpace, endNode)) AS annotated_assets
  FROM
    `raw_file_annotation`.`annotation_documents_tags`
  -- length() > 0 rather than != '': Spark copies these predicates to cdf_nodes through
  -- the join, and DMS rejects a pushed-down space filter on ''.
  WHERE
    status = 'Approved'
    AND length(startNode) > 0
    AND length(startNodeSpace) > 0
    AND length(endNode) > 0
    AND length(endNodeSpace) > 0
  GROUP BY
    startNodeSpace,
    startNode
)

SELECT
  a.space,
  a.externalId,
  -- 1000 is the CDF limit for a list of direct relations
  slice(
    CASE
      WHEN f.assets IS NULL THEN a.annotated_assets
      ELSE array_distinct(concat(f.assets, a.annotated_assets))
    END, 1, 1000
  ) AS assets
FROM
  approved AS a
  INNER JOIN cdf_nodes(
    '{{ fileSchemaSpace }}'
    , '{{ fileExternalId }}'
    , '{{ fileVersion }}'
  ) AS f
    ON f.space = a.space
    AND f.externalId = a.externalId
