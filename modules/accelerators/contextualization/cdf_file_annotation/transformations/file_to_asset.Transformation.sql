-- =============================================================================
-- FILE TO ASSET TRANSFORMATION
-- =============================================================================
-- This transformation populates the 'assets' property on File instances
-- based on approved diagram annotations.
--
-- Data Sources:
--   - RAW table: annotation_documents_tags (approved asset annotations)
--   - Destination: File view
--
-- Logic:
--   1. Query the asset annotations RAW table for approved annotations
--   2. Filter for status='Approved'
--   3. Group by file external ID
--   4. Aggregate asset references into an array
--   5. Update File instances with the assets property
-- =============================================================================

SELECT
  startNode AS externalId,
  
  -- Asset References: Aggregate approved asset annotations into an array
  -- Using slice to limit to 1000 elements (CDF limit for direct relations list)
  slice(
    collect_set(
      node_reference('{{ targetEntityInstanceSpace }}', endNode)
    ), 1, 1000
  ) AS assets

FROM
  `{{ rawDb }}`.`{{ rawTableDocTag }}`

WHERE
  -- Only include approved annotations
  status = 'Approved'
  -- Ensure valid file and asset references
  AND startNode IS NOT NULL
  AND startNode != ''
  AND endNode IS NOT NULL
  AND endNode != ''

GROUP BY
  startNode
