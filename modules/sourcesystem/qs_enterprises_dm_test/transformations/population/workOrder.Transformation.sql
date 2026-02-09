-- =============================================================================
-- WORK ORDER POPULATION TRANSFORMATION
-- =============================================================================
-- Loads work order data from SAP RAW into the enterprise WorkOrder view.
-- Source data: {{ rawSourceDatabase }}.workorder
--
-- WorkOrder implements: CogniteMaintenanceOrder, CogniteActivity,
--   CogniteSchedulable, CogniteSourceable, CogniteDescribable
--
-- Column mapping (RAW -> WorkOrder view):
--   sourceId                   -> externalId, sourceId
--   WORKORDER_TITLE            -> name
--   WORKORDER_DESC             -> description
--   WORKORDER_NUMBER           -> objectNumber (custom)
--   WORKORDER_STATUS           -> status (CogniteMaintenanceOrder)
--   WORKORDER_MAITENANCETYPE   -> type (CogniteMaintenanceOrder)
--   WORKORDER_PRIORITYDESC     -> priorityDescription (CogniteMaintenanceOrder)
--   WORKORDER_SCHEDULEDSTART   -> scheduledStartTime (CogniteSchedulable)
--   WORKORDER_DUEDATE          -> scheduledEndTime (CogniteSchedulable)
--   WORKORDER_PLANNEDSTART     -> startTime (CogniteSchedulable)
--   WORKORDER_COMPLETIONDATE   -> endTime (CogniteSchedulable)
--   WORKORDER_CREATEDDATE      -> sourceCreatedTime (CogniteSourceable)
-- =============================================================================

select
  -- Identity
  cast(`sourceId` as STRING)                               as externalId,

  -- Describable (CogniteDescribable)
  cast(`WORKORDER_TITLE` as STRING)                        as name,
  cast(`WORKORDER_DESC` as STRING)                         as description,

  -- Sourceable (CogniteSourceable)
  cast(`sourceId` as STRING)                               as sourceId,
  'SAP Work Orders'                                        as sourceContext,
  cast(`WORKORDER_CREATEDDATE` as TIMESTAMP)               as sourceCreatedTime,

  -- Schedulable (CogniteSchedulable)
  cast(`WORKORDER_SCHEDULEDSTART` as TIMESTAMP)            as scheduledStartTime,
  cast(`WORKORDER_DUEDATE` as TIMESTAMP)                   as scheduledEndTime,
  cast(`WORKORDER_PLANNEDSTART` as TIMESTAMP)              as startTime,
  cast(`WORKORDER_COMPLETIONDATE` as TIMESTAMP)            as endTime,

  -- MaintenanceOrder (CogniteMaintenanceOrder)
  cast(`WORKORDER_STATUS` as STRING)                       as status,
  cast(`WORKORDER_MAITENANCETYPE` as STRING)               as type,
  cast(`WORKORDER_PRIORITYDESC` as STRING)                 as priorityDescription,

  -- Custom (WorkOrder container)
  cast(`WORKORDER_NUMBER` as STRING)                       as objectNumber

from
  `{{ rawSourceDatabase }}`.`workorder`
where
  `sourceId` is not null
