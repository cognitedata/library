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

      -- Add Tags conditionally (>= 2 dashes in tag name)
      case when length(regexp_replace(dump.`WMT_TAG_NAME`, '[^-]', '')) >= 2
           then array('DetectInDiagrams')
           else array()
      end as tags,

      -- Add aliases conditionally (>= 2 dashes in tag name)
      case when length(regexp_replace(dump.`WMT_TAG_NAME`, '[^-]', '')) >= 2
           then array(
                  cast(dump.`WMT_TAG_NAME` as STRING),
                  regexp_replace(cast(dump.`WMT_TAG_NAME` as STRING), '^[^-]+-', '')
                )
           else array()
      end as aliases,

      -- Parent logic
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
