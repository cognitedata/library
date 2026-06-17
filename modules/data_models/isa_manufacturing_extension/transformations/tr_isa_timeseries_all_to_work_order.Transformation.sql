WITH changedWorkOrderIds AS (
  SELECT DISTINCT CAST(workOrderId AS STRING) AS workOrderId
  FROM `{{ rawDatabase }}`.`isa_timeseries`
  WHERE workOrderId IS NOT NULL AND workOrderId <> ''
    AND is_new('{{ rawDatabase }}', 'isa_timeseries')
  UNION
  SELECT DISTINCT CAST(workOrderId AS STRING) AS workOrderId
  FROM `{{ rawDatabase }}`.`isa_work_order`
  WHERE is_new('{{ rawDatabase }}', 'isa_work_order')
),
woTs AS (
  SELECT
    CAST(workOrderId AS STRING) AS external_id,
    COLLECT_SET(node_reference('{{ instance_space }}', CAST(`key` AS STRING))) AS timeSeries
  FROM `{{ rawDatabase }}`.`isa_timeseries`
  WHERE workOrderId IS NOT NULL AND workOrderId <> ''
  GROUP BY workOrderId
)
SELECT
  CAST(w.workOrderId AS STRING) AS externalId,
  CAST(w.workOrderId AS STRING) AS workOrderId,
  CAST(w.workOrderNumber AS STRING) AS workOrderNumber,
  woTs.timeSeries AS timeSeries
FROM `{{ rawDatabase }}`.`isa_work_order` w
INNER JOIN changedWorkOrderIds c ON CAST(w.workOrderId AS STRING) = c.workOrderId
LEFT JOIN woTs ON CAST(w.workOrderId AS STRING) = woTs.external_id
