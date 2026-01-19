-- =============================================================================
-- ASSET TRANSFORMATION SQL
-- =============================================================================
-- This SQL supports two modes:
--   1. COMMON MODE (default): Uses WMT:<name> external IDs, no tags/aliases
--   2. FILE_ANNOTATION MODE: Uses ast_<id> external IDs, includes tags/aliases
--
-- To switch modes:
--   - Comment/uncomment the sections marked with [COMMON] or [FILE_ANNOTATION]
--   - Ensure only ONE mode is active at a time
-- =============================================================================

-- =============================================================================
-- [COMMON MODE] - Parent Lookup via JOIN on WMT_TAG_NAME
-- Comment this CTE when using FILE_ANNOTATION mode
-- =============================================================================
with parentLookup as (
  select
    concat('WMT:', cast(d1.`WMT_TAG_NAME` as STRING)) as externalId,
    node_reference('{{ instanceSpace }}', concat('WMT:', cast(d2.`WMT_TAG_NAME` as STRING))) as parent
  from
    `{{ rawSourceDatabase }}`.`dump` as d1
  join
    `{{ rawSourceDatabase }}`.`dump` as d2
  on
    d1.`WMT_TAG_ID_ANCESTOR` = d2.`WMT_TAG_ID`
  where
    isnotnull(d1.`WMT_TAG_NAME`) AND
    isnotnull(d2.`WMT_TAG_NAME`)
)
select
  concat('WMT:', cast(d3.`WMT_TAG_NAME` as STRING)) as externalId,
  parentLookup.parent,
  cast(`WMT_TAG_NAME` as STRING) as name,
  cast(`WMT_TAG_DESC` as STRING) as description,
  cast(`WMT_TAG_ID` as STRING) as sourceId,
  cast(`WMT_TAG_CREATED_DATE` as TIMESTAMP) as sourceCreatedTime,
  cast(`WMT_TAG_UPDATED_DATE` as TIMESTAMP) as sourceUpdatedTime,
  cast(`WMT_TAG_UPDATED_BY` as STRING) as sourceUpdatedUser
from
  `{{ rawSourceDatabase }}`.`dump` as d3
left join
  parentLookup
on
  concat('WMT:', cast(d3.`WMT_TAG_NAME` as STRING)) = parentLookup.externalId
where
  isnotnull(d3.`WMT_TAG_NAME`)

-- =============================================================================
-- [FILE_ANNOTATION MODE] - Uses ast_<id> format with tags/aliases for diagram detection
-- Uncomment this entire section when FILE_ANNOTATION mode is needed
-- =============================================================================
/*
with root as (
  select
      'ast_VAL' as externalId,
      'VAL' as name,
      'Valhall platform' as description,
      null as sourceId,
      null as sourceCreatedTime,
      null as sourceUpdatedTime,
      null as sourceUpdatedUser,
      array() as tags,
      array() as aliases,
      null as parent
),
base as (
  select
      concat('ast_', cast(dump.`WMT_TAG_ID` as INT)) as externalId,
      cast(dump.`WMT_TAG_NAME` as STRING) as name,
      cast(dump.`WMT_TAG_DESC` as STRING) as description,
      cast(cast(dump.`WMT_TAG_ID` as INT) as STRING) as sourceId,
      cast(dump.`WMT_TAG_CREATED_DATE` as TIMESTAMP) as sourceCreatedTime,
      cast(dump.`WMT_TAG_UPDATED_DATE` as TIMESTAMP) as sourceUpdatedTime,
      cast(cast(dump.`WMT_TAG_UPDATED_BY` as INT) as STRING) as sourceUpdatedUser,

      -- Tags: Add 'DetectInDiagrams' for assets with >= 2 dashes (for file annotation matching)
      case when length(regexp_replace(dump.`WMT_TAG_NAME`, '[^-]', '')) >= 2
           then array('DetectInDiagrams')
           else array()
      end as tags,

      -- Aliases: Add name variations for diagram text matching
      case when length(regexp_replace(dump.`WMT_TAG_NAME`, '[^-]', '')) >= 2
           then array(
                  cast(dump.`WMT_TAG_NAME` as STRING),
                  regexp_replace(cast(dump.`WMT_TAG_NAME` as STRING), '^[^-]+-', '')
                )
           else array()
      end as aliases,

      -- Parent logic: Hardcoded root attachment for ID 681760, else use ancestor
      case when dump.`WMT_TAG_ID` = 681760
           then node_reference('{{ instanceSpace }}', 'ast_VAL')
           else node_reference('{{ instanceSpace }}', concat('ast_', cast(dump.`WMT_TAG_ID_ANCESTOR` as INT)))
      end as parent
  from
      `{{ rawSourceDatabase }}`.`dump` as dump
  where
      isnotnull(dump.`WMT_TAG_ID`)
)

-- Root always first, then base records
select * from root
union all
select * from base
*/