--
-- CFIHOS: Link WorkOrderOperation.timeSeries via shared tag (mainAsset).
--
SELECT
  op.externalId,
  CASE
    WHEN max(CASE WHEN ts.externalId IS NOT NULL AND ts.externalId != '' THEN 1 ELSE 0 END) = 0 THEN NULL
    ELSE collect_set(node_reference('{{ instance_space }}', cast(ts.externalId as string)))
  END AS timeSeries
FROM
  cdf_data_models(
    "{{ space }}",
    "{{ data_model_external_id }}",
    "{{ dm_version }}",
    "WorkOrderOperation"
  ) op
LEFT JOIN
  cdf_data_models(
    "{{ space }}",
    "{{ data_model_external_id }}",
    "{{ dm_version }}",
    "TimeSeriesData"
  ) ts
ON
  ts.space = '{{ instance_space }}'
  AND op.mainAsset IS NOT NULL
  AND array_contains(ts.assets, op.mainAsset)
WHERE
  op.space = '{{ instance_space }}'
  AND op.externalId IS NOT NULL
GROUP BY
  op.externalId
