-- =============================================================================
-- ASSET POPULATION TRANSFORMATION
-- =============================================================================
-- Loads asset data from SAP/Workmate RAW dump into the enterprise Asset view.
-- Source data is loaded into {{ rawSourceDatabase }}.dump
--
-- Column mapping (RAW -> Asset view):
--   WMT_TAG_NAME        -> externalId (prefixed with 'WMT:'), name
--   WMT_TAG_DESC        -> description
--   WMT_TAG_ID          -> sourceId
--   WMT_TAG_ID_ANCESTOR -> parent (resolved via join)
--   WMT_TAG_CREATED_DATE -> sourceCreatedTime
--   WMT_TAG_UPDATED_DATE -> sourceUpdatedTime
--   WMT_TAG_UPDATED_BY  -> sourceUpdatedUser
-- =============================================================================

-- Resolve parent references by joining on WMT_TAG_ID_ANCESTOR -> WMT_TAG_ID
with parentLookup as (
  select
    concat('WMT:', cast(child.`WMT_TAG_NAME` as STRING))   as childExternalId,
    node_reference(
      '{{ enterpriseInstanceSpace }}',
      concat('WMT:', cast(parent.`WMT_TAG_NAME` as STRING))
    )                                                        as parent
  from
    `{{ rawSourceDatabase }}`.`dump` as child
  join
    `{{ rawSourceDatabase }}`.`dump` as parent
  on
    child.`WMT_TAG_ID_ANCESTOR` = parent.`WMT_TAG_ID`
  where
    child.`WMT_TAG_NAME` is not null
    and parent.`WMT_TAG_NAME` is not null
)

select
  -- Identity
  concat('WMT:', cast(d.`WMT_TAG_NAME` as STRING))       as externalId,

  -- Hierarchy
  p.parent,

  -- Describable properties (CogniteDescribable)
  cast(d.`WMT_TAG_NAME` as STRING)                        as name,
  cast(d.`WMT_TAG_DESC` as STRING)                        as description,

  -- Sourceable properties (CogniteSourceable)
  cast(d.`WMT_TAG_ID` as STRING)                          as sourceId,
  'Asset'                                                  as sourceContext,
  cast(d.`WMT_TAG_CREATED_DATE` as TIMESTAMP)             as sourceCreatedTime,
  cast(d.`WMT_TAG_UPDATED_DATE` as TIMESTAMP)             as sourceUpdatedTime,
  cast(d.`WMT_TAG_UPDATED_BY` as STRING)                  as sourceUpdatedUser

from
  `{{ rawSourceDatabase }}`.`dump` as d
left join
  parentLookup as p
on
  concat('WMT:', cast(d.`WMT_TAG_NAME` as STRING)) = p.childExternalId
where
  d.`WMT_TAG_NAME` is not null
