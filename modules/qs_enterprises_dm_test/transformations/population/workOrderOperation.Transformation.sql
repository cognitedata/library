-- =============================================================================
-- WORK ORDER OPERATION POPULATION TRANSFORMATION
-- =============================================================================
-- Loads work order operations (work items) from SAP RAW into the enterprise
-- WorkOrderOperation view, including the relation to parent WorkOrder.
-- Source data: {{ rawSourceDatabase }}.workitem + {{ rawSourceDatabase }}.workorder
--
-- WorkOrderOperation implements: CogniteOperation, CogniteActivity,
--   CogniteSchedulable, CogniteSourceable, CogniteDescribable
--
-- Column mapping (RAW -> WorkOrderOperation view):
--   sourceId                   -> externalId, sourceId
--   WORKORDER_TASKNAME/ITEMINFO -> name
--   WORKORDER_NUMBER + fields  -> description
--   WORKORDER_STATUS           -> status (CogniteOperation)
--   maintenanceOrder           -> link to WorkOrder (CogniteOperation)
--   WORKORDER_ITEMNAME         -> objectNumber (custom)
-- =============================================================================

-- Deduplicate workitems (source data may contain duplicates)
with unique_workitem as (
  select
    *,
    row_number() over (partition by `sourceId` order by `sourceId`) as rn
  from
    `{{ rawSourceDatabase }}`.`workitem`
),
-- Deduplicate workorders for the join (WORKORDER_NUMBER may not be unique)
unique_workorder as (
  select
    *,
    row_number() over (partition by `WORKORDER_NUMBER` order by `sourceId`) as rn
  from
    `{{ rawSourceDatabase }}`.`workorder`
)

select
  -- Identity
  cast(wi.`sourceId` as STRING)                            as externalId,

  -- Describable (CogniteDescribable)
  cast(
    coalesce(wi.`WORKORDER_TASKNAME`, wi.`WORKORDER_ITEMINFO`)
    as STRING
  )                                                        as name,
  cast(
    concat_ws(
      ' - ',
      wi.`WORKORDER_NUMBER`,
      wi.`WORKORDER_ITEMTAGHIGHCRITICAL`,
      wi.`WORKORDER_ITEMINFO`
    ) as STRING
  )                                                        as description,

  -- Sourceable (CogniteSourceable)
  cast(wi.`sourceId` as STRING)                            as sourceId,
  'SAP Work Order Operations'                              as sourceContext,

  -- Operation (CogniteOperation) - link to parent WorkOrder
  node_reference(
    '{{ enterpriseInstanceSpace }}',
    cast(wo.`sourceId` as STRING)
  )                                                        as maintenanceOrder,

  -- Custom (WorkOrderOperation container)
  cast(wi.`WORKORDER_ITEMNAME` as STRING)                  as objectNumber

from
  unique_workitem as wi
left join
  unique_workorder as wo
on
  wi.`WORKORDER_NUMBER` = wo.`WORKORDER_NUMBER`
  and wo.rn = 1
where
  wi.`sourceId` is not null
  and wi.rn = 1
