select
  concat('WMT:', cast(`WMT_TAG_NAME` as STRING)) as externalId,
  cast(`WMT_TAG_NAME` as STRING) as name,
  cast(`WMT_TAG_DESC` as STRING) as description,
  cast(cast(`WMT_TAG_ID` as INT) as STRING) as sourceId, 
  to_timestamp(`WMT_TAG_CREATED_DATE`, 'dd/MM/yyyy HH:mm') as sourceCreatedTime,
  to_timestamp(`WMT_TAG_UPDATED_DATE`, 'dd/MM/yyyy HH:mm') as sourceUpdatedTime,
  cast(cast(`WMT_TAG_UPDATED_BY` as INT) as STRING) as sourceUpdatedUser,
  cast(cast(`WMT_CONTRACTOR_ID` as INT) as STRING) as manufacturer,
  cast(cast(`WMT_TAG_GLOBALID` as BIGINT) as STRING) as serialNumber
from
  `{{ rawSourceDatabase }}`.`dump`
where
  isnotnull(`WMT_TAG_NAME`) 
  and cast(`WMT_CATEGORY_ID` as INT) != 1157
