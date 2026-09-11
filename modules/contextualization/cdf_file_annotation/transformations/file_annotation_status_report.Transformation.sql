-- =============================================================================
-- FILE ANNOTATION STATUS REPORT
-- =============================================================================
-- Builds a per-file summary of diagram tag matching from the file annotation RAW
-- tables written by fn_file_annotation_finalize and fn_file_annotation_promote.
--
-- Sources:
--   - {{ rawTableDocTag }}     Regular diagram detect → asset links
--   - {{ rawTableDocPattern }} Pattern-mode detections and promote outcomes
--
-- Match categories (aligned with the Annotation Quality dashboard):
--   matched   : status = 'Approved' (tag linked to an asset external ID)
--   unmatched : pattern rows with status 'Rejected' or 'Suggested' (no asset link)
--
-- Scope:
--   Limited to file instances in {{ fileInstanceSpace }} (startNodeSpace).
--   Rows with a missing startNodeSpace are treated as belonging to that space.
-- =============================================================================

WITH regular_asset_annotations AS (
  SELECT
      cast(startNode AS STRING) AS fileExternalId
    , coalesce(cast(startNodeSpace AS STRING), '{{ fileInstanceSpace }}') AS fileSpace
    , cast(startSourceId AS STRING) AS fileSourceId
    , cast(startNodeText AS STRING) AS tagText
    , cast(endNode AS STRING) AS assetExternalId
    , trim(cast(status AS STRING)) AS status
    , cast(sourceUpdatedTime AS STRING) AS sourceUpdatedTime
  FROM `{{ rawDb }}`.`{{ rawTableDocTag }}`
  WHERE coalesce(cast(startNodeSpace AS STRING), '{{ fileInstanceSpace }}') = '{{ fileInstanceSpace }}'
    AND startNode IS NOT NULL
    AND trim(cast(startNode AS STRING)) != ''
    AND startNodeText IS NOT NULL
    AND trim(cast(startNodeText AS STRING)) != ''
    AND endNode IS NOT NULL
    AND trim(cast(endNode AS STRING)) != ''
),

pattern_asset_annotations AS (
  SELECT
      cast(startNode AS STRING) AS fileExternalId
    , coalesce(cast(startNodeSpace AS STRING), '{{ fileInstanceSpace }}') AS fileSpace
    , cast(startSourceId AS STRING) AS fileSourceId
    , cast(startNodeText AS STRING) AS tagText
    , cast(endNode AS STRING) AS assetExternalId
    , trim(cast(status AS STRING)) AS status
    , cast(sourceUpdatedTime AS STRING) AS sourceUpdatedTime
  FROM `{{ rawDb }}`.`{{ rawTableDocPattern }}`
  WHERE coalesce(cast(startNodeSpace AS STRING), '{{ fileInstanceSpace }}') = '{{ fileInstanceSpace }}'
    AND startNode IS NOT NULL
    AND trim(cast(startNode AS STRING)) != ''
    AND startNodeText IS NOT NULL
    AND trim(cast(startNodeText AS STRING)) != ''
    -- Skip file-to-file pattern hits; this report is asset/tag focused.
    -- Depends on launchFunction.fileResourceProperty and targetEntitiesResourceProperty
    -- being unset in extraction_pipelines/ep_file_annotation.config.yaml (default empty).
    -- If either is set, finalize/promote populate endNodeResourceType from that mapping
    -- instead of the view external ID, so this filter may not exclude file-to-file rows
    -- and they can show up as unmatched tags without an obvious signal.
    AND coalesce(cast(endNodeResourceType AS STRING), '{{ targetEntityExternalId }}') != '{{ fileExternalId }}'
),

all_annotations AS (
  SELECT * FROM regular_asset_annotations
  UNION ALL
  SELECT * FROM pattern_asset_annotations
),

classified AS (
  SELECT
      fileExternalId
    , fileSpace
    , fileSourceId
    , tagText
    , assetExternalId
    , status
    , sourceUpdatedTime
    , CASE
        WHEN status = 'Approved' THEN 'matched'
        WHEN status = 'Rejected' THEN 'unmatched'
        WHEN status = 'Suggested' THEN 'unmatched'
        ELSE 'other'
      END AS matchCategory
  FROM all_annotations
),

matched_rows AS (
  SELECT
      fileExternalId
    , fileSpace
    , max(fileSourceId) AS fileSourceId
    , concat_ws(
        '; '
      , sort_array(
          collect_set(concat(tagText, ' -> ', assetExternalId))
        )
      ) AS matchedTagsAndAssets
    , count(DISTINCT tagText) AS matchedCount
    , max(sourceUpdatedTime) AS lastMatchedUpdatedTime
  FROM classified
  WHERE matchCategory = 'matched'
  GROUP BY
      fileExternalId
    , fileSpace
),

unmatched_rows AS (
  SELECT
      fileExternalId
    , fileSpace
    , max(fileSourceId) AS fileSourceId
    , concat_ws('; ', sort_array(collect_set(tagText))) AS unmatchedTags
    , count(DISTINCT tagText) AS unmatchedCount
    , max(sourceUpdatedTime) AS lastUnmatchedUpdatedTime
  FROM classified
  WHERE matchCategory = 'unmatched'
  GROUP BY
      fileExternalId
    , fileSpace
),

file_keys AS (
  SELECT fileExternalId, fileSpace FROM matched_rows
  UNION
  SELECT fileExternalId, fileSpace FROM unmatched_rows
)

SELECT
    concat(fileKeys.fileSpace, ':', fileKeys.fileExternalId) AS key
  , fileKeys.fileExternalId
  , fileKeys.fileSpace
  , coalesce(matched_rows.fileSourceId, unmatched_rows.fileSourceId) AS fileSourceId
  , coalesce(matched_rows.matchedTagsAndAssets, '') AS matchedTagsAndAssets
  , coalesce(unmatched_rows.unmatchedTags, '') AS unmatchedTags
  , coalesce(matched_rows.matchedCount, 0) AS matchedCount
  , coalesce(unmatched_rows.unmatchedCount, 0) AS unmatchedCount
  , greatest(
        coalesce(matched_rows.lastMatchedUpdatedTime, '1970-01-01T00:00:00')
      , coalesce(unmatched_rows.lastUnmatchedUpdatedTime, '1970-01-01T00:00:00')
    ) AS lastUpdatedTime
FROM file_keys AS fileKeys
LEFT JOIN matched_rows
  ON fileKeys.fileExternalId = matched_rows.fileExternalId
 AND fileKeys.fileSpace = matched_rows.fileSpace
LEFT JOIN unmatched_rows
  ON fileKeys.fileExternalId = unmatched_rows.fileExternalId
 AND fileKeys.fileSpace = unmatched_rows.fileSpace
