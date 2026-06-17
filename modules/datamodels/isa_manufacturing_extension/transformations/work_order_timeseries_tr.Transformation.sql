with work_orders as (
  select 
    externalId,work_order_id,work_order_number 
  from 
    cdf_data_models('sp_isa_manufacturing', 'ISA_Manufacturing', 'v1', 'WorkOrder')
),
wo_ts as (
  SELECT
    cast(work_order_id as string) AS externalId,
    collect_set(node_reference('{{ isaInstanceSpace }}', cast(key as string))) AS timeSeries
  FROM `ISA_Manufacturing`.`isa_timeseries`
  WHERE work_order_id IS NOT NULL AND work_order_id <> ''
  GROUP BY work_order_id
)
select 
  work_orders.externalId as externalId,
  work_orders.work_order_id as work_order_id,
  work_orders.work_order_number as work_order_number,
  wo_ts.timeSeries as timeSeries
from work_orders left join wo_ts on work_orders.externalId=wo_ts.externalId
