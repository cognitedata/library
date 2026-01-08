select
  cast(`sourceId` as STRING) as externalId,
  cast(`sourceId` as STRING) as sourceId,
  cast(`WORKORDER_DESC` as STRING) as description,
  cast(`WORKORDER_TITLE` as STRING) as name,
  cast(`WORKORDER_STATUS` as STRING) as status,
  cast(`WORKORDER_SCHEDULEDSTART` as TIMESTAMP) as scheduledStartTime,
  cast(`WORKORDER_DUEDATE` as TIMESTAMP) as scheduledEndTime,
  cast(`WORKORDER_PLANNEDSTART` as TIMESTAMP) as startTime,
  cast(`WORKORDER_COMPLETIONDATE` as TIMESTAMP) as endTime,
  cast(`WORKORDER_CREATEDDATE` as TIMESTAMP) as sourceCreatedTime,
  cast(`WORKORDER_MAITENANCETYPE` as STRING) as type,
  cast(`WORKORDER_PRIORITYDESC` as STRING) as priorityDescription,
  'SAP Maintenance Orders' as sourceContext,
  transform(
    split(`assetExternalIds`, ';'),
    x -> regexp_replace(x, '^WMT:', '')
  ) as sysTagsFound
from
  `{{ rawSourceDatabase }}`.`workorder`
where
  isnotnull(`sourceId`)
